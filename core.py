import argparse
import time
import sys
import logging
import os
import queue
from typing import Dict, Optional, List, Any

# Import project modules
from buffer_manager import BufferManager
from mavlink_handler.mavlink_handler_copter import MAVLinkHandlerCopter
from socketio_connection import SocketIOConnection

# Global instances (consider class structure later if complexity grows)
mav_copter: Optional[MAVLinkHandlerCopter] = None
socket_client: Optional[SocketIOConnection] = None
buffer: Optional[BufferManager] = None
logger: Optional[logging.Logger] = None

def setup_logging(log_level_str="INFO", log_file="mavlink_bridge.log"):
    """Configures file and console logging."""
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    # More detailed format
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-8s - %(message)s')
    log_dir = os.path.dirname(log_file)

    # Ensure log directory exists
    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
            print(f"Created log directory: {log_dir}") # Use print before logging is fully setup
        except OSError as e:
            print(f"CRITICAL: Error creating log directory {log_dir}: {e}. Logging to current directory.", file=sys.stderr)
            log_file = os.path.basename(log_file) # Fallback to current dir

    # Configure file handler
    log_handler = logging.FileHandler(log_file, mode='a') # Append mode
    log_handler.setFormatter(log_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    if root_logger.hasHandlers(): # Avoid duplicate handlers if re-run in interactive session
        print("Clearing existing log handlers.")
        root_logger.handlers.clear()

    root_logger.setLevel(log_level) # Set root level (e.g., DEBUG to capture all)
    root_logger.addHandler(log_handler)

    # Configure console handler (INFO level for user feedback)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    # Set console level separately - INFO is usually good for console
    console_handler.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)

    # Get logger instance for the main module
    logger_instance = logging.getLogger(__name__)
    logger_instance.info(f"Logging configured. Level: {log_level_str}, File: '{log_file}'")
    return logger_instance # Return the specific logger

def parse_args():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='MAVLink to WebSocket Bridge')
    # Server settings
    parser.add_argument('--srv-ptc', default='http', help='WebSocket server protocol (http/https)')
    parser.add_argument('--srv-host', default='localhost', help='WebSocket server host')
    parser.add_argument('--srv-port', default='3000', help='WebSocket server port')
    parser.add_argument('--srv-token', default='bridge', help='WebSocket server token (optional)')
    # MAVLink settings
    parser.add_argument('--mv-url', default='udp:localhost:14550', help='MAVLink connection URL (e.g., udp:ip:port, /dev/ttyUSB0:baud)')
    parser.add_argument('--mv-source-system', default=255, type=int, help='MAVLink source system ID for this bridge')
    # Buffer settings
    parser.add_argument('--buffer-size', default=50, type=int, help='Max messages in buffer before flush')
    parser.add_argument('--flush-timeout', default=1.0, type=float, help='Max seconds between messages before flush (reduced default)')
    # Logging settings
    parser.add_argument('--log-file', default='mavlink_bridge.log', help='Path to log file')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Logging level for file')
    # Configuration settings
    parser.add_argument('--socket-check-interval', default=5.0, type=float, help='Socket.IO connection check interval (s)')
    parser.add_argument('--socket-max-disconnect', default=30.0, type=float, help='Max Socket.IO disconnect duration before exit (s)')
    parser.add_argument('--loop-sleep', default=0.01, type=float, help='Main loop sleep duration (s)')

    return parser.parse_args()

# --- Callback Functions ---
def handle_mavlink_command(data: Dict[str, Any]) -> int: # Return type updated
    """Callback for forwarding immediate commands from Socket.IO."""
    global mav_copter, logger
    if mav_copter:
        return mav_copter.send_command(data) # Call the command method which returns 1, 2, or False
    if logger:
        logger.error("Cannot forward command: MAVLink connection unavailable.")
    return False # Return False if connection unavailable

def handle_mavlink_mission_download() -> Optional[List[Dict[str, Any]]]:
    """Callback for triggering mission download from Socket.IO."""
    global mav_copter, logger
    if mav_copter:
        return mav_copter.mission_get()
    if logger:
        logger.error("Cannot download mission: MAVLink connection unavailable.")
    return None

def handle_mavlink_mission_upload(items: List[Dict[str, Any]]) -> bool:
    """Callback for triggering mission upload from Socket.IO."""
    global mav_copter, logger
    if mav_copter:
        return mav_copter.mission_set(items)
    if logger:
        logger.error("Cannot upload mission: MAVLink connection unavailable.")
    return False

# --- Main Execution ---

def main():
    """Initializes components and runs the main application loop."""
    global mav_copter, socket_client, buffer, logger # Declare globals used

    args = parse_args()
    logger = setup_logging(log_level_str=args.log_level, log_file=args.log_file)
    logger.info("--- MAVLink Bridge Starting ---")
    logger.debug(f"Arguments: {vars(args)}") # Log args at debug level

    # Configuration from arguments
    server_protocol = args.srv_ptc
    server_host = args.srv_host
    server_port = args.srv_port
    server_token = args.srv_token
    SERVER_URL = f'{server_protocol}://{server_host}:{server_port}?user={server_token}'

    MAVLINK_URL = args.mv_url
    BUFFER_SIZE = args.buffer_size
    FLUSH_TIMEOUT = args.flush_timeout
    MAVLINK_SOURCE_SYSTEM = args.mv_source_system
    LOOP_SLEEP_DURATION = args.loop_sleep

    try:
        # Initialize MAVLink Connection
        logger.info("Initializing MAVLink connection...")
        mav_copter = MAVLinkHandlerCopter(MAVLINK_URL, source_system=MAVLINK_SOURCE_SYSTEM, logger=logger)
        logger.info("MAVLink connection initialization attempted.")

        # Initialize Socket.IO Connection using appropriate handlers
        logger.info("Initializing Socket.IO connection...")
        socket_client = SocketIOConnection(
            server_url=SERVER_URL,
            handler_command=handle_mavlink_command,
            handler_mission_download=handle_mavlink_mission_download,
            handler_mission_upload=handle_mavlink_mission_upload,
            logger=logger,
            check_interval=args.socket_check_interval,
            max_disconnect_time=args.socket_max_disconnect
        )

        # Attempt Socket.IO connection
        if not socket_client.connect():
            logger.critical("Initial Socket.IO connection failed. Exiting.")
            if mav_copter: mav_copter.close() # Attempt cleanup
            sys.exit(1)
        logger.info("Socket.IO connection established.")

        # Initialize Buffer Manager
        logger.info("Initializing Buffer Manager...")
        buffer = BufferManager(BUFFER_SIZE, FLUSH_TIMEOUT, logger=logger)
        logger.info("Buffer Manager initialized.")

    except ConnectionError as e:
        logger.critical(f"Fatal Initialization Error (MAVLink): {str(e)}")
        if socket_client: socket_client.disconnect()
        if mav_copter: mav_copter.close()
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Fatal Initialization Error: {str(e)}", exc_info=True)
        if socket_client: socket_client.disconnect()
        if mav_copter: mav_copter.close()
        sys.exit(1)

    # --- Main application loop ---
    logger.info("Starting main application loop...")

    while True:
        try:
            # 1. Check for permanent MAVLink failure signaled by receiver thread
            if mav_copter and mav_copter.connection_failed_permanently:
                logger.critical("Fatal: MAVLink connection failed permanently. Exiting.")
                if socket_client: socket_client.disconnect()
                sys.exit(1)

            # 2. Process general messages from MAVLink receiver queue (non-blocking)
            if mav_copter:
                try:
                    msg = mav_copter.received_messages.get_nowait()
                    if msg and buffer:
                        if buffer.add_message(msg):
                            if socket_client: socket_client.flush_buffer(buffer)
                except queue.Empty:
                    pass # No message
                except Exception as q_err:
                    logger.error(f"Error getting message from MAVLink general queue: {q_err}", exc_info=True)

            # 3. Check buffer timeout
            if buffer and buffer.check_timeout():
                if socket_client: socket_client.flush_buffer(buffer)

            # 4. Check for persistent Socket.IO disconnection
            if socket_client and not socket_client.check_persistent_disconnect():
                logger.critical("Exiting due to persistent Socket.IO disconnection.")
                if mav_copter: mav_copter.close()
                sys.exit(1)

            # Sleep briefly
            time.sleep(LOOP_SLEEP_DURATION)

        except (OSError, BrokenPipeError, AttributeError) as e:
            logger.error(f"Communication or attribute error in main loop: {str(e)}. Checking connections.", exc_info=True)
            if not mav_copter or mav_copter.connection_failed_permanently:
                logger.warning("MAVLink connection appears down in main loop check.")
            if not socket_client or not socket_client.client.connected:
                logger.warning("Socket.IO connection appears down in main loop check.")
            time.sleep(1)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Exiting gracefully...")
            break

        except Exception as e:
            logger.error(f"Unexpected error in main loop: {str(e)}", exc_info=True)
            time.sleep(0.5)

    # --- Cleanup phase ---
    logger.info("Shutting down...")
    if socket_client:
        socket_client.disconnect()
    if mav_copter:
        mav_copter.close()

    logger.info("--- MAVLink Bridge Stopped ---")
    sys.exit(0)

if __name__ == "__main__":
    main()
