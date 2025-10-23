#!/usr/bin/env python3
"""
Simple command spawner that redirects output to a file.
Takes a command and arguments, executes them, and saves output to a specified file.
"""
import pty
import sys
import argparse
import os


def main():
    parser = argparse.ArgumentParser(
        description="Spawn command and redirect output to file",
        usage="%(prog)s -o OUTPUT_FILE -- COMMAND [ARGS...]"
    )
    parser.add_argument("-o", "--output", required=True, help="Output file to redirect stdout and stderr")
    parser.add_argument("--binary", action="store_true", 
                       help="Write output in binary mode (preserves all bytes exactly)")
    parser.add_argument("command_args", nargs=argparse.REMAINDER, 
                       help="Command and arguments to execute (after --)")
    
    parsed_args = parser.parse_args()
    
    # Check if -- separator was used and remove it if present
    cmd = parsed_args.command_args
    if cmd and cmd[0] == '--':
        cmd = cmd[1:]
    
    # Validate that we have a command to execute
    if not cmd:
        parser.error("No command specified. Use -- before the command: -o output.log -- command args")
    
    print(f"Executing: {' '.join(cmd)}")
    print(f"Output will be saved to: {parsed_args.output}")
    
    try:
        # Open output file for writing (binary or text mode)
        # Binary mode: preserves all bytes exactly (best for ANSI sequences)
        # Text mode: line buffered for real-time streaming to websocket server
        file_mode = 'wb' if parsed_args.binary else 'w'
        with open(parsed_args.output, file_mode, buffering=0 if parsed_args.binary else 1) as output_file:
            # Define custom read function to capture output
            def read_and_write(fd):
                try:
                    # Use larger buffer to avoid splitting ANSI sequences
                    data = os.read(fd, 4096)
                    if data:
                        if parsed_args.binary:
                            # Binary mode: write raw bytes (preserves everything exactly)
                            output_file.write(data)
                        else:
                            # Text mode: decode and write (preserves ANSI sequences)
                            output_file.write(data.decode('utf-8', errors='replace'))
                        output_file.flush()
                        return data
                except OSError:
                    pass
                return b''
            
            # Execute command using pty.spawn
            exit_code = pty.spawn(cmd, read_and_write)
            
        print(f"Command completed with exit code: {exit_code}")
        
        # Return the same exit code as the spawned process
        return exit_code
        
    except FileNotFoundError as e:
        if 'No such file or directory' in str(e) and parsed_args.output in str(e):
            print(f"Error: Cannot create output file '{parsed_args.output}' - directory may not exist", file=sys.stderr)
        else:
            print(f"Error: Command '{cmd[0] if cmd else 'unknown'}' not found", file=sys.stderr)
        return 127
    except PermissionError:
        print(f"Error: Permission denied executing '{cmd[0] if cmd else 'unknown'}'", file=sys.stderr)
        return 126
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
