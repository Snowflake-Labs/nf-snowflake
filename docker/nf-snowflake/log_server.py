#!/usr/bin/env python3
"""
Log File WebSocket Server

This script monitors a log file and streams its contents via WebSocket 
for real-time monitoring. Always reads from the beginning of the file and
follows new content (tail -f behavior) until client disconnects.
Preserves ANSI escape sequences for terminal colors.
Designed for single client connection.
"""

import asyncio
import aiohttp
from aiohttp import web
from aiohttp.web_log import AccessLogger
import os
import json
import argparse
import sys
import logging
import time
from typing import Optional
from pathlib import Path

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

class LogFileWebSocketServer:
    def __init__(self, log_file: str, exit_code_file: Optional[str] = None, 
                 host: str = "0.0.0.0", port: int = 8766):
        self.log_file = log_file
        self.exit_code_file = exit_code_file
        self.host = host
        self.port = port
        self.client_websocket = None
        self.tail_task: Optional[asyncio.Task] = None
        self.exit_code_watch_task: Optional[asyncio.Task] = None
        self.running = False
        self.exit_code: Optional[int] = None

    async def handle_websocket(self, request):
        """Handle the single WebSocket client connection"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        if self.client_websocket is not None:
            logger.warning("Client already connected, rejecting new connection")
            await ws.close(code=aiohttp.WSMsgType.CLOSE, message=b"Server busy")
            return ws
            
        logger.info(f"Client connected: {request.remote}")
        self.client_websocket = ws
        
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f'WebSocket error: {ws.exception()}')
                    break
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            logger.info(f"Client disconnected: {request.remote}")
            self.client_websocket = None
            
        return ws
    
    async def send_message(self, message_type: str, data: str = "", **kwargs):
        """Send a message to the connected client"""
        if self.client_websocket is None:
            return
            
        message = {
            "type": message_type,
            "data": data,
            "timestamp": asyncio.get_event_loop().time(),
            **kwargs
        }
        
        try:
            await self.client_websocket.send_str(json.dumps(message))
        except Exception as e:
            logger.debug(f"Client connection closed while sending message: {e}")
            self.client_websocket = None
    
    async def wait_for_file(self, timeout: float = 30.0):
        """Wait for the log file to exist"""
        start_time = time.time()
        while not os.path.exists(self.log_file):
            if time.time() - start_time > timeout:
                raise FileNotFoundError(f"Log file {self.log_file} not found after {timeout}s")
            await asyncio.sleep(0.1)
        logger.info(f"Log file found: {self.log_file}")
    
    async def watch_exit_code_file(self):
        """Watch for exit code file and read its contents when it appears"""
        if not self.exit_code_file:
            return
        
        logger.info(f"Watching for exit code file: {self.exit_code_file}")
        
        try:
            # Wait for exit code file to appear
            while self.running and not os.path.exists(self.exit_code_file):
                await asyncio.sleep(0.1)
            
            if not self.running:
                return
            
            # Read exit code from file
            logger.info(f"Exit code file detected: {self.exit_code_file}")
            
            # Give it a moment to ensure file is fully written
            await asyncio.sleep(0.1)
            
            try:
                with open(self.exit_code_file, 'r') as f:
                    exit_code_str = f.read().strip()
                    self.exit_code = int(exit_code_str)
                    logger.info(f"Read exit code: {self.exit_code}")
            except (ValueError, IOError) as e:
                logger.warning(f"Error reading exit code file: {e}, defaulting to 0")
                self.exit_code = 0
            
            # Stop the log streaming
            self.running = False
            logger.info("Exit code file detected, stopping log streaming")
            
        except Exception as e:
            logger.error(f"Error watching exit code file: {e}")
            self.exit_code = 1
    
    async def tail_log_file(self):
        """Read and stream log file contents to WebSocket client"""
        try:
            # Wait for file to exist if it doesn't
            if not os.path.exists(self.log_file):
                logger.info(f"Waiting for log file: {self.log_file}")
                await self.send_message("status", status="waiting", message=f"Waiting for {self.log_file}...")
                await self.wait_for_file()
            
            await self.send_message("status", status="started", file=self.log_file)
            
            # Open file in binary mode to preserve ANSI sequences exactly
            mode = 'rb'
            with open(self.log_file, mode) as f:
                # Always start from beginning
                logger.info("Reading from beginning of file")
                
                line_count = 0
                while self.running:
                    line = f.readline()
                    
                    if line:
                        # Decode bytes to string, preserving ANSI sequences
                        line_str = line.decode('utf-8', errors='replace')
                        
                        # Send line to client (strip trailing newline for cleaner transport)
                        await self.send_message("output", line_str.rstrip('\n\r'))
                        line_count += 1
                    else:
                        # No new data - check if client is still connected
                        if self.client_websocket is None:
                            logger.info(f"Client disconnected, stopping log streaming (sent {line_count} lines)")
                            break
                        
                        # Wait for new data
                        await asyncio.sleep(0.1)
                        
                        # Check if file was rotated or truncated
                        try:
                            current_pos = f.tell()
                            file_size = os.path.getsize(self.log_file)
                            
                            if current_pos > file_size:
                                # File was truncated, reopen from beginning
                                logger.info("File truncated, reopening...")
                                f.seek(0)
                        except (OSError, IOError) as e:
                            logger.warning(f"Error checking file: {e}")
                
                # Send completion message with exit code
                if self.exit_code is not None:
                    await self.send_message("status", status="completed", 
                                          lines_sent=line_count, exit_code=self.exit_code)
                    logger.info(f"Log streaming completed, sent {line_count} lines, exit code: {self.exit_code}")
                else:
                    await self.send_message("status", status="completed", lines_sent=line_count)
                    logger.info(f"Log streaming completed, sent {line_count} lines")
                
        except FileNotFoundError as e:
            logger.error(f"Log file not found: {e}")
            await self.send_message("status", status="error", error=str(e))
        except Exception as e:
            logger.error(f"Error tailing log file: {e}")
            await self.send_message("status", status="error", error=str(e))
    
    async def healthz_handler(self, request):
        """Health check endpoint"""
        return web.Response(text="OK", status=200)
    
    async def start_server(self):
        """Start the HTTP/WebSocket server and begin tailing log"""
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
        logger.info(f"Monitoring log file: {self.log_file}")
        if self.exit_code_file:
            logger.info(f"Watching exit code file: {self.exit_code_file}")
        logger.info("Waiting for client connection...")
        
        self.running = True
        
        try:
            # Wait for client to connect
            while self.client_websocket is None and self.running:
                await asyncio.sleep(0.1)
            
            if not self.running:
                return 0
            
            logger.info("Client connected, starting log streaming...")
            
            # Start both log tailing and exit code watching concurrently
            tasks = [asyncio.create_task(self.tail_log_file())]
            
            if self.exit_code_file:
                tasks.append(asyncio.create_task(self.watch_exit_code_file()))
            
            # Wait for either task to complete (exit code detection will stop log tailing)
            await asyncio.gather(*tasks)
            
            # Keep server alive for a moment to send final messages
            await asyncio.sleep(2)
            
            return self.exit_code if self.exit_code is not None else 0
            
        except Exception as e:
            logger.error(f"Server error: {e}")
            return 1
        finally:
            self.running = False
            await runner.cleanup()

def main():
    parser = argparse.ArgumentParser(
        description="Stream log file contents via WebSocket",
        usage="%(prog)s -f LOG_FILE [OPTIONS]"
    )
    parser.add_argument("-f", "--file", required=True, help="Log file to monitor")
    parser.add_argument("-e", "--exit-code-file", 
                       help="File to watch for process exit code (stops streaming when detected)")
    parser.add_argument("--host", default="0.0.0.0", help="WebSocket server host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket server port (default: 8766)")
    
    args = parser.parse_args()
    
    # Validate log file path
    log_file = args.file
    if not os.path.exists(log_file):
        logger.warning(f"Log file does not exist yet: {log_file}")
        logger.info("Will wait for file to be created...")
    
    server = LogFileWebSocketServer(
        log_file=log_file,
        exit_code_file=args.exit_code_file,
        host=args.host,
        port=args.port
    )
    
    try:
        exit_code = asyncio.run(server.start_server())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

