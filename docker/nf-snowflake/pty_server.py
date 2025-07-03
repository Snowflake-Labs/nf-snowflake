#!/usr/bin/env python3
"""
Nextflow PTY WebSocket Server

This script runs Nextflow in a pseudo-terminal (PTY) using subprocess
and streams the output via WebSocket for real-time monitoring.
Designed for single client connection.
"""

import asyncio
import websockets
import pty
import os
import subprocess
import json
import argparse
import sys
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NextflowPTYServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        self.host = host
        self.port = port
        self.process: Optional[subprocess.Popen] = None
        self.master_fd: Optional[int] = None
        self.client_websocket = None
        
    async def handle_client(self, websocket):
        """Handle the single WebSocket client connection"""
        if self.client_websocket is not None:
            logger.warning("Client already connected, rejecting new connection")
            await websocket.close(code=1013, reason="Server busy")
            return
            
        logger.info(f"Client connected: {websocket.remote_address}")
        self.client_websocket = websocket
        
        try:
            await websocket.wait_closed()
        finally:
            logger.info(f"Client disconnected: {websocket.remote_address}")
            self.client_websocket = None
    
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
            await self.client_websocket.send(json.dumps(message))
        except websockets.exceptions.ConnectionClosed:
            logger.info("Client connection closed while sending message")
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
    
    async def run_nextflow(self, nextflow_args: list):
        """Run Nextflow in a PTY using subprocess"""
        try:
            # Create PTY
            self.master_fd, slave_fd = pty.openpty()
            
            # Set environment for ANSI output
            env = os.environ.copy()
            #env.update({
            #    'NXF_ANSI_LOG': 'true',
            #    'NXF_OPTS': '-Djansi.passthrough=true',
            #    'TERM': 'xterm-256color',
            #    'FORCE_COLOR': '1'
            #})
            
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
    
    async def start_server(self, nextflow_args: list):
        """Start the WebSocket server and run Nextflow"""
        logger.info(f"Starting WebSocket server on {self.host}:{self.port}")
        
        # Start WebSocket server
        server = await websockets.serve(self.handle_client, self.host, self.port)
        logger.info("WebSocket server started, waiting for client connection...")
        
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
            server.close()
            await server.wait_closed()

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
