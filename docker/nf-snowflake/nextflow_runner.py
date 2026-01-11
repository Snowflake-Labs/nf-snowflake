#!/usr/bin/env python3
"""
Nextflow PTY Runner

Runs a nextflow command in a PTY and captures output to a file.
Designed for use in multi-container SPCS setups where output needs to be
written to a shared volume for streaming by another container.
"""

import sys
import os
import pty
import select
import subprocess
import threading
import time


def tail_nextflow_log():
    """Background thread to tail .nextflow.log and emit to stderr"""
    log_file_path = ".nextflow.log"
    
    # Wait for log file to exist
    while not os.path.exists(log_file_path):
        time.sleep(0.1)
    
    try:
        with open(log_file_path, 'r', encoding='utf-8', errors='replace') as f:
            # Move to end of file to start tailing
            f.seek(0, 2)
            
            while True:
                line = f.readline()
                if line:
                    # Emit line to stderr (remove newline since print adds one)
                    print(f"[.nextflow.log] {line.rstrip()}", file=sys.stderr, flush=True)
                else:
                    # No new line, wait a bit before checking again
                    time.sleep(0.1)
    except Exception as e:
        print(f"[.nextflow.log] Error tailing log: {e}", file=sys.stderr, flush=True)


def run_with_pty(command, output_file):
    """
    Run command in a PTY and capture output to file.
    
    Args:
        command: List of command arguments
        output_file: Path to file where output should be written
        
    Returns:
        Exit code of the command
    """
    # Start background thread to tail .nextflow.log
    tail_thread = threading.Thread(target=tail_nextflow_log, daemon=True)
    tail_thread.start()
    
    # Create PTY
    master_fd, slave_fd = pty.openpty()
    
    try:
        # Open output file
        with open(output_file, 'wb') as out_f:
            # Start the subprocess with PTY
            process = subprocess.Popen(
                command,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                preexec_fn=os.setsid,
                close_fds=True
            )
            
            # Close slave fd in parent process
            os.close(slave_fd)
            
            # Read from master and write to file
            while True:
                try:
                    # Use select to check if there's data to read
                    readable, _, _ = select.select([master_fd], [], [], 0.1)
                    
                    if readable:
                        try:
                            data = os.read(master_fd, 1024)
                            if data:
                                out_f.write(data)
                                out_f.flush()  # Flush immediately for real-time streaming
                            else:
                                # EOF
                                break
                        except OSError:
                            # Read error, process probably finished
                            break
                    
                    # Check if process has finished
                    if process.poll() is not None:
                        # Read any remaining data
                        try:
                            while True:
                                data = os.read(master_fd, 1024)
                                if not data:
                                    break
                                out_f.write(data)
                                out_f.flush()
                        except OSError:
                            pass
                        break
                        
                except KeyboardInterrupt:
                    process.terminate()
                    break
            
            # Wait for process to complete
            exit_code = process.wait()
            return exit_code
            
    finally:
        try:
            os.close(master_fd)
        except OSError:
            pass


def main():
    if len(sys.argv) < 2:
        print("Usage: nextflow_runner.py <command> [args...]", file=sys.stderr)
        sys.exit(1)
    
    command = sys.argv[1:]
    output_file = "/shared/nextflow.out"
    exit_code_file = "/shared/exit_code"
    
    # Run the command
    exit_code = run_with_pty(command, output_file)
    
    # Write exit code
    with open(exit_code_file, 'w') as f:
        f.write(str(exit_code))
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

