#!/usr/bin/env python3
"""
Nextflow WebSocket Log Server

This script streams logs from a file written by the main Nextflow command executor.
It continues running until the main Nextflow command executor exits and returns
the exit code back to the client.

Refactored from pty_server.py to work with file-based log streaming in a
multi-container Snowpark Container Services setup.
"""

import asyncio
import aiohttp
from aiohttp import web
from aiohttp.web_log import AccessLogger
import json
import argparse
import sys
import logging
import os
import time
from typing import Optional

class WebSocketServerFormatter(logging.Formatter):
    """Custom formatter that adds [websocket.server] tag to all log messages"""

    def format(self, record):
        # Get the original formatted message
        original_message = super().format(record)
        # Add the websocket.server tag
        return f"[websocket.server] {original_message}"


# Configure logging with custom formatter
handler = logging.StreamHandler(sys.stderr)
formatter = WebSocketServerFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Configure the root logger
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)


class NoHealthzAccessLogger(AccessLogger):
    """Custom access logger that ignores healthz endpoint requests"""

    def log(self, request, response, time):
        # Skip logging for healthz endpoint
        if request.path == '/healthz':
            return
        # Use default logging for all other requests
        super().log(request, response, time)


class NextflowLogServer:
    """
    WebSocket server that streams logs from files written by nextflow_runner.py.
    
    The server monitors:
    - /shared/nextflow.out: Live output from Nextflow
    - /shared/exit_code: Exit code written when process completes
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8765,
        log_file: str = "/shared/nextflow.out",
        exit_code_file: str = "/shared/exit_code"
    ):
        self.host = host
        self.port = port
        self.log_file = log_file
        self.exit_code_file = exit_code_file
        self.client_websocket: Optional[web.WebSocketResponse] = None
        self.streaming_task: Optional[asyncio.Task] = None
        self.exit_code: Optional[int] = None

    async def handle_websocket(self, request):
        """Handle WebSocket client connection"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        if self.client_websocket is not None:
            logger.warning("Client already connected, rejecting new connection")
            await ws.close(code=aiohttp.WSCloseCode.TRY_AGAIN_LATER, message=b"Server busy")
            return ws
            
        logger.info(f"Client connected: {request.remote}")
        self.client_websocket = ws
        
        # Send connected status
        await self.send_message("status", status="connected")
        
        # Start streaming logs
        self.streaming_task = asyncio.create_task(self.stream_logs())
        
        try:
            # Create a task to listen for client messages
            async def listen_for_messages():
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f'WebSocket error: {ws.exception()}')
                        break
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        # Client can send messages if needed (e.g., for control)
                        logger.debug(f"Received message from client: {msg.data}")
            
            message_task = asyncio.create_task(listen_for_messages())
            
            # Wait for either streaming to complete or message listening to end
            done, pending = await asyncio.wait(
                [self.streaming_task, message_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            # Cancel any remaining tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            logger.info("Streaming completed, closing connection")
            
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            logger.info(f"Client disconnected: {request.remote}")
            if self.streaming_task and not self.streaming_task.done():
                self.streaming_task.cancel()
                try:
                    await self.streaming_task
                except asyncio.CancelledError:
                    pass
            
            # Close the WebSocket connection gracefully
            if not ws.closed:
                await ws.close()
            
            self.client_websocket = None
            
        return ws
    
    async def send_message(self, message_type: str, data: str = "", **kwargs):
        """Send a message to the connected client"""
        if self.client_websocket is None:
            return
            
        message = {
            "type": message_type,
            "data": data,
            "timestamp": time.time(),
            **kwargs
        }
        
        try:
            await self.client_websocket.send_str(json.dumps(message))
        except Exception as e:
            logger.info(f"Client connection closed while sending message: {e}")
            self.client_websocket = None
    
    async def stream_logs(self):
        """
        Stream logs from the log file to the WebSocket client.
        Monitors exit_code_file to detect when process completes.
        """
        logger.info(f"Starting log streaming from {self.log_file}")
        
        # Wait for log file to exist (with timeout)
        wait_timeout = 300  # seconds
        wait_start = time.time()
        while not os.path.exists(self.log_file):
            if time.time() - wait_start > wait_timeout:
                error_msg = f"Timeout waiting for log file: {self.log_file}"
                logger.error(error_msg)
                await self.send_message("error", message=error_msg, code="TIMEOUT")
                return
            await asyncio.sleep(0.1)
        
        logger.info(f"Log file found: {self.log_file}")
        
        try:
            # Open log file and seek to beginning
            with open(self.log_file, 'rb') as f:
                # Track file position
                position = 0
                
                while True:
                    # Check if process has completed
                    if os.path.exists(self.exit_code_file) and self.exit_code is None:
                        try:
                            with open(self.exit_code_file, 'r') as exit_f:
                                self.exit_code = int(exit_f.read().strip())
                                logger.info(f"Process completed with exit code: {self.exit_code}")
                        except (ValueError, IOError) as e:
                            logger.warning(f"Failed to read exit code: {e}")
                            self.exit_code = 1
                    
                    # Read new data from file
                    f.seek(position)
                    data = f.read(4096)
                    
                    if data:
                        # Decode and send data
                        try:
                            output = data.decode('utf-8', errors='replace')
                            await self.send_message("output", output)
                            position = f.tell()
                        except Exception as e:
                            logger.error(f"Error sending output: {e}")
                            break
                    
                    # If process completed and no more data, finish
                    if self.exit_code is not None and not data:
                        # Send completion status
                        await self.send_message("status", status="completed", exit_code=self.exit_code)
                        logger.info("Log streaming completed")
                        break
                    
                    # Small delay to prevent busy waiting
                    if not data:
                        await asyncio.sleep(0.1)
                    
        except asyncio.CancelledError:
            logger.info("Log streaming cancelled")
            raise
        except Exception as e:
            logger.error(f"Error streaming logs: {e}")
            await self.send_message("error", message=f"Error streaming logs: {e}", code="STREAM_ERROR")
    
    async def healthz_handler(self, request):
        """Health check endpoint"""
        return web.Response(text="OK", status=200)
    
    async def start_server(self):
        """Start the HTTP/WebSocket server"""
        logger.info(f"Starting HTTP/WebSocket server on {self.host}:{self.port}")
        
        # Create aiohttp application
        app = web.Application()
        
        # Add routes
        app.router.add_get('/ws', self.handle_websocket)
        app.router.add_get('/healthz', self.healthz_handler)
        
        # Start server with custom access logger
        runner = web.AppRunner(app, access_log_class=NoHealthzAccessLogger)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        
        logger.info(f"Server started on {self.host}:{self.port}")
        logger.info("WebSocket endpoint: /ws")
        logger.info("Health check endpoint: /healthz")
        logger.info("Waiting for client connection and log streaming...")
        
        try:
            # Keep server running
            while True:
                await asyncio.sleep(3600)  # Sleep for an hour at a time
        except asyncio.CancelledError:
            logger.info("Server shutdown requested")
        finally:
            await runner.cleanup()


def main():
    parser = argparse.ArgumentParser(
        description="Stream Nextflow logs via WebSocket"
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="WebSocket server host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="WebSocket server port (default: 8765)"
    )
    parser.add_argument(
        "--log-file",
        default="/shared/nextflow.out",
        help="Path to log file to stream (default: /shared/nextflow.out)"
    )
    parser.add_argument(
        "--exit-code-file",
        default="/shared/exit_code",
        help="Path to exit code file (default: /shared/exit_code)"
    )
    
    args = parser.parse_args()
    
    server = NextflowLogServer(
        args.host,
        args.port,
        args.log_file,
        args.exit_code_file
    )
    
    try:
        asyncio.run(server.start_server())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

