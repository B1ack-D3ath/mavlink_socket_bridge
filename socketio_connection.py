import socketio
import logging
import time
from typing import Callable, Optional, List, Dict, Any

class SocketIOConnection:
    """
    Manages Socket.IO client connection, event handling, and message flushing.
    Includes persistent disconnect checks, refactored status emitting
    """

    def __init__(self,
                server_url: str,
                handler_command: Optional[Callable[[Dict[str, Any]], bool]] = None,
                handler_mission_download: Optional[Callable[[], Optional[List[Dict[str, Any]]]]] = None,
                handler_mission_upload: Optional[Callable[[List[Dict[str, Any]]], bool]] = None,
                logger: Optional[logging.Logger] = None,
                check_interval: float = 5.0,
                max_disconnect_time: float = 30.0):
        """
        Initializes the Socket.IO client.

        Args:
            server_url (str): The URL of the Socket.IO server.
            handler_command (callable, optional): Handler for 'request_command'. Should return int (1, 2, or False).
            handler_mission_download (callable, optional): Handler for 'request_mission_download'. Should return list or None.
            handler_mission_upload (callable, optional): Handler for 'request_mission_upload'. Should return bool.
            logger (logging.Logger, optional): Logger instance.
            check_interval (float): How often to check connection status (seconds).
            max_disconnect_time (float): Max duration disconnected before check fails (seconds).
        """
        self.server_url = server_url
        self.handler_command = handler_command
        self.handler_mission_download = handler_mission_download
        self.handler_mission_upload = handler_mission_upload
        self.logger = logger or logging.getLogger(__name__)

        self._check_interval = check_interval
        self._max_disconnect_time = max_disconnect_time
        self._last_check_time = time.time()
        self._disconnect_duration = 0.0

        self.client = socketio.Client(
            reconnection=True,
            reconnection_attempts=10, # Or -1 for infinite
            reconnection_delay=1,
            reconnection_delay_max=5
            # logger=True, engineio_logger=True # Enable for detailed socketio debugging if needed
        )
        self._register_handlers()

    # --- Connection Management ---

    def connect(self):
        """
        Establishes the initial connection to the Socket.IO server.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            self.logger.info(f"Attempting Socket.IO connection to {self.server_url}...")
            self.client.connect(self.server_url, wait_timeout=10)
            return True
        except socketio.exceptions.ConnectionError as e:
            self.logger.error(f"Socket.IO connection failed: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during Socket.IO connection: {str(e)}", exc_info=True)
            return False

    def disconnect(self):
        """Disconnects the client if connected."""
        try:
            if self.client.connected:
                self._unregister_handlers()
                self.client.disconnect()
                self.logger.info("Socket.IO client disconnected.")
                return True
            return False
        except Exception as e:
            self.logger.error(f"Error during Socket.IO disconnect: {str(e)}", exc_info=True)
            return False

    def check_persistent_disconnect(self):
        """
        Checks if the client has been disconnected longer than the threshold.

        Returns:
            bool: True if connection is okay or still within threshold,
                False if disconnected for too long (signals main loop to exit).
        """
        current_time = time.time()
        if current_time - self._last_check_time > self._check_interval:
            if not self.client.connected:
                self._disconnect_duration += (current_time - self._last_check_time)
                self.logger.warning(f"Socket.IO still disconnected. Accumulated duration: {self._disconnect_duration:.1f}s / {self._max_disconnect_time}s")
                if self._disconnect_duration >= self._max_disconnect_time:
                    self.logger.critical(f"Socket.IO disconnected threshold ({self._max_disconnect_time}s) exceeded.")
                    return False
            else:
                if self._disconnect_duration > 0:
                    self.logger.info("Socket.IO connection check: OK (was previously disconnected).")
                self._disconnect_duration = 0 # Reset duration if connected
            self._last_check_time = current_time # Update check time regardless of status
        return True

    # --- Event Handling ---

    def _register_handlers(self):
        """Registers essential Socket.IO event handlers."""
        self.client.on('connect', self._on_connect)
        self.client.on('disconnect', self._on_disconnect)
        self.client.on('connect_error', self._on_connect_error)

        # Register handlers provided during initialization
        if self.handler_command:
            self.client.on('request_command', self._on_request_command)
        if self.handler_mission_download:
            self.client.on('request_mission_download', self._on_request_mission_download)
        if self.handler_mission_upload:
            self.client.on('request_mission_upload', self._on_request_mission_upload)
        self.logger.info("Socket.IO event handlers registered.")

    def _unregister_handlers(self):
        """Unregisters event handlers."""
        # Use a list of events to unregister for cleaner code
        events_to_unregister = ['connect', 'disconnect', 'connect_error']
        if self.handler_command: events_to_unregister.append('request_command')
        if self.handler_mission_download: events_to_unregister.append('request_mission_download')
        if self.handler_mission_upload: events_to_unregister.append('request_mission_upload')

        for event in events_to_unregister:
            try:
                self.client.off(event)
            except KeyError: # Ignore if handler wasn't registered
                pass
        self.logger.info("Socket.IO event handlers unregistered.")

    def _on_connect(self):
        """Callback for successful connection."""
        self.logger.info(f"Socket.IO connected to {self.server_url} (SID: {self.client.sid})")
        self._disconnect_duration = 0.0
        self._last_check_time = time.time()

    def _on_disconnect(self):
        """Callback for disconnection."""
        self.logger.warning("Socket.IO disconnected. Attempting automatic reconnection...")

    def _on_connect_error(self, data):
        """Callback for connection errors during initial connection attempts."""
        self.logger.error(f"Socket.IO connection attempt failed: {data or 'No details provided'}")

    def _emit_status(self, event_name: str, status_dict: Dict[str, Any]):
        """Helper method to emit status updates, checking connection and handling errors."""
        if not self.client.connected:
            self.logger.warning(f"Cannot emit status '{event_name}': Socket.IO not connected. Status: {status_dict}")
            return False
        try:
            # Log only success status for brevity, error details are logged elsewhere
            self.logger.info(f"Emitting status '{event_name}': success={status_dict.get('success', False)}")
            self.client.emit(event_name, status_dict)
            return True
        except Exception as e:
            self.logger.error(f"Failed to emit status '{event_name}': {e}", exc_info=True)
            return False

    def _on_request_command(self, data):
        """Handles 'request_command' event from the server."""
        event_name = 'status_request_command' # Define status event name
        if not self.handler_command:
            self.logger.warning("Received 'request_command' but no handler_command configured.")
            self._emit_status(event_name, {'success': False, 'error': 'Bridge command handler not configured'})
            return

        self.logger.info("Received 'request_command'. Triggering command...")
        try:
            # Handler is expected to return 1 (single success), 2 (list success), or False
            success_code = self.handler_command(data)
            if success_code == 1:
                self._emit_status(event_name, {'success': True, 'list': False})
            elif success_code == 2:
                self._emit_status(event_name, {'success': True, 'list': True})
            else: # Includes False
                # Handler should log specific reasons for failure
                self._emit_status(event_name, {'success': False, 'error': 'Command processing failed on bridge/vehicle'})
        except Exception as e:
            self.logger.error(f"Error occurred during command request processing: {str(e)}", exc_info=True)
            self._emit_status(event_name, {'success': False, 'error': f'Internal bridge error: {str(e)}'})


    def _on_request_mission_download(self, data):
        """Handles 'request_mission_download' event from the server."""
        event_name = 'status_request_mission_download' # Define status event name
        if not self.handler_mission_download:
            self.logger.warning("Received 'request_mission_download' but no handler_mission_download configured.")
            self._emit_status(event_name, {'success': False, 'error': 'Bridge download handler not configured'})
            return

        self.logger.info("Received 'request_mission_download'. Triggering download...")
        try:
            # Handler is expected to return list or None
            mission_list = self.handler_mission_download()
            if mission_list is not None: # Check for None explicitly
                self.logger.info(f"Mission download successful. Sending {len(mission_list)} items.")
                self._emit_status(event_name, {'success': True, 'items': mission_list})
            else:
                self.logger.error("Mission download failed or returned None.")
                self._emit_status(event_name, {'success': False, 'error': 'Mission download failed on bridge/vehicle'})
        except Exception as e:
            self.logger.error(f"Error occurred during mission download request processing: {str(e)}", exc_info=True)
            self._emit_status(event_name, {'success': False, 'error': f'Internal bridge error: {str(e)}'})


    def _on_request_mission_upload(self, data):
        """Handles 'request_mission_upload' event from the server."""
        event_name = 'status_request_mission_upload' # Define status event name
        if not self.handler_mission_upload:
            self.logger.warning("Received 'request_mission_upload' but no handler_mission_upload configured.")
            self._emit_status(event_name, {'success': False, 'error': 'Bridge upload handler not configured'})
            return

        # Basic validation of incoming data
        if not isinstance(data, list):
            self.logger.error(f"Invalid 'request_mission_upload' data: Expected list, got {type(data)}")
            self._emit_status(event_name, {'success': False, 'error': f'Invalid data format: Expected list, got {type(data).__name__}'})
            return
        # Allow empty list upload (effectively clears mission)

        self.logger.info(f"Received 'request_mission_upload' with {len(data)} items. Triggering upload...")
        try:
            # Handler is expected to return True/False
            success = self.handler_mission_upload(data)
            if success:
                self.logger.info("Mission upload reported successful by handler.")
                self._emit_status(event_name, {'success': True})
            else:
                self.logger.error("Mission upload reported failed by handler.")
                self._emit_status(event_name, {'success': False, 'error': 'Mission upload failed on bridge/vehicle'})
        except Exception as e:
            self.logger.error(f"Error occurred during mission upload request processing: {str(e)}", exc_info=True)
            self._emit_status(event_name, {'success': False, 'error': f'Internal bridge error: {str(e)}'})

    # --- Buffer Flushing ---

    def flush_buffer(self, buffer_manager):
        """
        Emits buffered messages to the server via 'mavlink_message'.
        Sends the list of messages directly.

        Args:
            buffer_manager (BufferManager): The buffer manager instance.

        Returns:
            bool: True if flushed successfully or buffer was empty, False on error.
        """
        if buffer_manager.is_empty():
            return True
        if not self.client.connected:
            self.logger.warning("Cannot flush buffer: Socket.IO client not connected.")
            return False # Do not clear buffer

        try:
            buffer_content = buffer_manager.get_buffer_content()
            message_count = len(buffer_content)

            if message_count > 0:
                # Emit the whole buffer content (list of dicts)
                self.client.emit('mavlink_message', buffer_content)
                self.logger.info(f"Flushed {message_count} messages to server.")
            else:
                self.logger.debug("Flush called but buffer was empty.")

            buffer_manager.clear_buffer() # Clear buffer after successful emit attempt
            return True

        except Exception as e:
            # Log error if emission fails
            self.logger.error(f"Socket.IO emit error during flush: {str(e)}", exc_info=True)
            # Do NOT clear buffer here, allow retry
            return False

    # --- Handler Setters ---

    def set_handler_command(self, handler_command):
        """Updates the handler for 'request_command' events."""
        self.handler_command = handler_command
        try: self.client.off('request_command')
        except KeyError: pass
        if handler_command:
            self.client.on('request_command', self._on_request_command)
            self.logger.info("Updated 'request_command' handler.")
        else:
            self.logger.info("Removed 'request_command' handler.")

    def set_handler_mission_download(self, handler_mission_download):
        """Updates the handler for 'request_mission_download' events."""
        self.handler_mission_download = handler_mission_download
        try: self.client.off('request_mission_download')
        except KeyError: pass
        if handler_mission_download:
            self.client.on('request_mission_download', self._on_request_mission_download)
            self.logger.info("Updated 'request_mission_download' handler.")
        else:
            self.logger.info("Removed 'request_mission_download' handler.")

    def set_handler_mission_upload(self, handler_mission_upload):
        """Updates the handler for 'request_mission_upload' events."""
        self.handler_mission_upload = handler_mission_upload
        try: self.client.off('request_mission_upload')
        except KeyError: pass
        if handler_mission_upload:
            self.client.on('request_mission_upload', self._on_request_mission_upload)
            self.logger.info("Updated 'request_mission_upload' handler.")
        else:
            self.logger.info("Removed 'request_mission_upload' handler.")

    # Optional combined setter (can be added later if needed)
    def set_handlers(self, command=None, mission_download=None, mission_upload=None):
        self.set_handler_command(command)
        self.set_handler_mission_download(mission_download)
        self.set_handler_mission_upload(mission_upload)

