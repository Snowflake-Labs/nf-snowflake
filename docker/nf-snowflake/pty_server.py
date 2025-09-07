#!/usr/bin/env python3
"""
Nextflow PTY WebSocket Server

This script runs Nextflow in a pseudo-terminal (PTY) using subprocess
and streams the output via WebSocket for real-time monitoring.
Designed for single client connection.
"""

import asyncio
import aiohttp
from aiohttp import web
from aiohttp.web_log import AccessLogger
import pty
import os
import subprocess
import json
import argparse
import sys
import logging
import threading
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

class NextflowPTYServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        self.host = host
        self.port = port
        self.process: Optional[subprocess.Popen] = None
        self.master_fd: Optional[int] = None
        self.client_websocket = None
        self.log_tail_thread: Optional[threading.Thread] = None
        self.log_tail_stop_event = threading.Event()

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
            logger.info(f"Client connection closed while sending message: {e}")
            self.client_websocket = None
    
    async def read_pty_output(self):
        """Read output from PTY and send to WebSocket client"""
        loop = asyncio.get_event_loop()
        
        while self.process and self.process.poll() is None:
            try:
                # Use asyncio to read from the PTY without blocking
                data = await loop.run_in_executor(None, self._read_pty_data)
                if data:
                    output = data.decode('utf-8', errors='replace')
                    await self.send_message("output", output)
                else:
                    # Small delay to prevent busy waiting
                    await asyncio.sleep(0.01)
            except Exception as e:
                logger.error(f"Error reading PTY: {e}")
                break
    
    def _read_pty_data(self) -> bytes:
        """Read data from PTY file descriptor (blocking call)"""
        if self.master_fd is None:
            return b""
        try:
            return os.read(self.master_fd, 1024)
        except (OSError, BlockingIOError):
            return b""
    
    def _tail_nextflow_log(self):
        """Background thread to tail .nextflow.log file and emit to stderr"""
        log_file_path = ".nextflow.log"

        # Wait for log file to exist
        while not os.path.exists(log_file_path) and not self.log_tail_stop_event.is_set():
            time.sleep(0.1)

        if self.log_tail_stop_event.is_set():
            return

        try:
            with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
                # Move to end of file to start tailing
                f.seek(0, 2)  # Seek to end

                while not self.log_tail_stop_event.is_set():
                    line = f.readline()
                    if line:
                        # Emit line to stderr (remove newline since print adds one)
                        print(f"[.nextflow.log] {line.rstrip()}", file=sys.stderr)
                    else:
                        # No new line, wait a bit before checking again
                        time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error tailing .nextflow.log: {e}")

    def _start_log_tail_thread(self):
        """Start the background thread for tailing .nextflow.log"""
        if self.log_tail_thread is None or not self.log_tail_thread.is_alive():
            self.log_tail_stop_event.clear()
            self.log_tail_thread = threading.Thread(target=self._tail_nextflow_log, daemon=True)
            self.log_tail_thread.start()
            logger.info("Started background thread for tailing .nextflow.log")

    def _stop_log_tail_thread(self):
        """Stop the background thread for tailing .nextflow.log"""
        if self.log_tail_thread and self.log_tail_thread.is_alive():
            self.log_tail_stop_event.set()
            self.log_tail_thread.join(timeout=5)
            logger.info("Stopped background thread for tailing .nextflow.log")

    async def run_nextflow(self, nextflow_args: list):
        """Run Nextflow in a PTY using subprocess"""
        try:
            # Create PTY
            self.master_fd, slave_fd = pty.openpty()
            
            # Set environment for ANSI output
            env = os.environ.copy()
            
            # Build command
            logger.info(f"Executing: {' '.join(nextflow_args)}")
            
            await self.send_message("status", status="starting", command=' '.join(nextflow_args))
            
            # Start subprocess with PTY
            self.process = subprocess.Popen(
                nextflow_args,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                env=env,
                preexec_fn=os.setsid,  # Create new session
                close_fds=True
            )
            
            # Close slave fd in parent process
            os.close(slave_fd)
            
            await self.send_message("status", status="started", pid=self.process.pid)
            
            # Start the background thread for tailing .nextflow.log
            self._start_log_tail_thread()
            
            # Start reading PTY output
            read_task = asyncio.create_task(self.read_pty_output())
            
            # Wait for process to complete
            loop = asyncio.get_event_loop()
            exit_code = await loop.run_in_executor(None, self.process.wait)
            
            # Cancel reading task
            read_task.cancel()
            try:
                await read_task
            except asyncio.CancelledError:
                pass
            
            await self.send_message("status", status="completed", exit_code=exit_code)
            logger.info(f"Nextflow completed with exit code: {exit_code}")
            
            return exit_code
            
        except Exception as e:
            logger.error(f"Error running Nextflow: {e}")
            await self.send_message("status", status="error", error=str(e))
            return 1
        
        finally:
            # Stop the background log tail thread
            self._stop_log_tail_thread()
            
            if self.master_fd:
                try:
                    os.close(self.master_fd)
                except OSError:
                    pass
                self.master_fd = None
            
            if self.process:
                # Ensure process is terminated
                if self.process.poll() is None:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
                        self.process.wait()
    
    async def healthz_handler(self, request):
        """Health check endpoint"""
        return web.Response(text="OK", status=200)
    
    async def start_server(self, nextflow_args: list):
        """Start the HTTP/WebSocket server and run Nextflow"""
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
        logger.info("Waiting for client connection...")
        
        try:
            # Wait for client to connect
            while self.client_websocket is None:
                await asyncio.sleep(0.1)
            
            logger.info("Client connected, starting Nextflow...")
            
            # Run Nextflow
            exit_code = await self.run_nextflow(nextflow_args)
            
            # Keep server alive for a moment to send final messages
            await asyncio.sleep(2)
            
            return exit_code
            
        finally:
            await runner.cleanup()

def main():
    parser = argparse.ArgumentParser(description="Run Nextflow in PTY with WebSocket streaming")
    parser.add_argument("--host", default="0.0.0.0", help="WebSocket server host")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket server port")
    parser.add_argument("nextflow_args", nargs=argparse.REMAINDER, help="Nextflow command arguments")
    
    args = parser.parse_args()
    
    if not args.nextflow_args:
        print("Error: No Nextflow arguments provided", file=sys.stderr)
        sys.exit(1)
    
    # Remove the first '--' if present (from argparse REMAINDER)
    if args.nextflow_args and args.nextflow_args[0] == '--':
        args.nextflow_args = args.nextflow_args[1:]
    
    server = NextflowPTYServer(args.host, args.port)
    
    try:
        exit_code = asyncio.run(server.start_server(args.nextflow_args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 
