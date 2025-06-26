import socketio
import logging
import time
from typing import Callable, Optional, List, Dict, Any

class SocketIOConnection:
    """
    Manages Socket.IO client connection, event handling, and message flushing.
    Includes persistent disconnect checks, and handlers for commands, missions, and operations.
    """

    def __init__(self,
                server_url: str,
                handler_command: Optional[Callable[[Dict[str, Any]], bool]] = None,
                handler_mission_download: Optional[Callable[[], Optional[List[Dict[str, Any]]]]] = None,
                handler_mission_upload: Optional[Callable[[List[Dict[str, Any]]], bool]] = None,
                handler_start_operation: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
                handler_stop_operation: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
                logger: Optional[logging.Logger] = None,
                check_interval: float = 5.0,
                max_disconnect_time: float = 30.0):
        """
        Initializes the Socket.IO client.

        Args:
            server_url (str): The URL of the Socket.IO server.
            handler_command (callable, optional): Handler for 'request_command'.
            handler_mission_download (callable, optional): Handler for 'request_mission_download'.
            handler_mission_upload (callable, optional): Handler for 'request_mission_upload'.
            handler_start_operation (callable, optional): Handler for 'request_start_operation'.
            handler_stop_operation (callable, optional): Handler for 'request_stop_operation'.
            logger (logging.Logger, optional): Logger instance.
            check_interval (float): How often to check connection status (seconds).
            max_disconnect_time (float): Max duration disconnected before check fails (seconds).
        """
        self.server_url = server_url
        self.handler_command = handler_command
        self.handler_mission_download = handler_mission_download
        self.handler_mission_upload = handler_mission_upload
        self.handler_start_operation = handler_start_operation
        self.handler_stop_operation = handler_stop_operation
        self.logger = logger or logging.getLogger(__name__)

        self._check_interval = check_interval
        self._max_disconnect_time = max_disconnect_time
        self._last_check_time = time.time()
        self._disconnect_duration = 0.0

        self.client = socketio.Client(
            reconnection=True,
            reconnection_attempts=-1,
            reconnection_delay=1,
            reconnection_delay_max=5
        )
        self._register_handlers()

    # --- Connection Management ---
    def connect(self) -> bool:
        """Establishes the initial connection to the Socket.IO server."""
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

    def disconnect(self) -> bool:
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

    def check_persistent_disconnect(self) -> bool:
        """Checks if the client has been disconnected longer than the threshold."""
        current_time = time.time()
        if current_time - self._last_check_time > self._check_interval:
            if not self.client.connected:
                self._disconnect_duration += (current_time - self._last_check_time)
                self.logger.warning(f"Socket.IO still disconnected. Accumulated duration: {self._disconnect_duration:.1f}s / {self._max_disconnect_time}s")
                if self._disconnect_duration >= self._max_disconnect_time:
                    self.logger.critical(f"Socket.IO disconnected threshold ({self._max_disconnect_time}s) exceeded.")
                    return False
            else:
                if self._disconnect_duration > 0: self.logger.info("Socket.IO connection re-established.")
                self._disconnect_duration = 0
            self._last_check_time = current_time
        return True

    # --- Event Handling ---
    def _register_handlers(self):
        """Registers essential Socket.IO event handlers."""
        self.client.on('connect', self._on_connect)
        self.client.on('disconnect', self._on_disconnect)
        self.client.on('connect_error', self._on_connect_error)

        if self.handler_command: self.client.on('request_command', self._on_request_command)
        if self.handler_mission_download: self.client.on('request_mission_download', self._on_request_mission_download)
        if self.handler_mission_upload: self.client.on('request_mission_upload', self._on_request_mission_upload)
        if self.handler_start_operation: self.client.on('request_start_operation', self._on_request_start_operation)
        if self.handler_stop_operation: self.client.on('request_stop_operation', self._on_request_stop_operation)
        self.logger.info("Socket.IO event handlers registered.")

    def _unregister_handlers(self):
        """Unregisters event handlers upon disconnection."""
        events = ['connect', 'disconnect', 'connect_error', 'request_command', 
                  'request_mission_download', 'request_mission_upload',
                  'request_start_operation', 'request_stop_operation']
        for event in events:
            try:
                self.client.off(event)
            except KeyError: pass
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

    def emit_status(self, event_name: str, status_dict: Dict[str, Any]):
        """Public helper method to emit status updates, checking connection and handling errors."""
        if not self.client.connected:
            self.logger.warning(f"Cannot emit status '{event_name}': Socket.IO not connected. Status: {status_dict}")
            return
        
        try:
            self.logger.info(f"Emitting status '{event_name}': {status_dict}")
            self.client.emit(event_name, status_dict)
        
        except Exception as e:
            self.logger.error(f"Failed to emit status '{event_name}': {e}", exc_info=True)

    def emit_response(self, event_name: str, response_dict: Dict[str, Any]):
        """Public helper method to emit status updates, checking connection and handling errors."""
        if not self.client.connected:
            self.logger.warning(f"Cannot emit status '{event_name}': Socket.IO not connected. Status: {response_dict}")
            return
        
        try:
            self.logger.info(f"Emitting status '{event_name}': {response_dict}")
            self.client.emit(event_name, response_dict)
        
        except Exception as e:
            self.logger.error(f"Failed to emit status '{event_name}': {e}", exc_info=True)

    def _on_request_command(self, data):
        """Handles 'request_command' event from the server."""
        event_name = 'response_command'
        if not self.handler_command:
            self.logger.warning("Received 'request_command' but no handler_command configured.")
            self.emit_response(event_name, {'success': False, 'error': 'Bridge command handler not configured'})
            return
        
        try:
            success_code = self.handler_command(data)
            if success_code == 1: self.emit_response(event_name, {'success': True, 'list': False})
            elif success_code == 2: self.emit_response(event_name, {'success': True, 'list': True})
            else: self.emit_response(event_name, {'success': False, 'error': 'Command processing failed on bridge/vehicle'})
        
        except Exception as e:
            self.logger.error(f"Error occurred during command request processing: {str(e)}", exc_info=True)
            self.emit_response(event_name, {'success': False, 'error': f'Internal bridge error: {str(e)}'})

    def _on_request_mission_download(self, data):
        """Handles 'request_mission_download' event from the server."""
        event_name = 'response_mission_download'
        if not self.handler_mission_download:
            self.logger.warning("Received 'request_mission_download' but no handler configured.")
            self.emit_response(event_name, {'success': False, 'error': 'Bridge download handler not configured'})
            return
        
        try:
            mission_list = self.handler_mission_download()
            if mission_list is not None: self.emit_response(event_name, {'success': True, 'items': mission_list})
            else: self.emit_response(event_name, {'success': False, 'error': 'Mission download failed on bridge/vehicle'})
        
        except Exception as e:
            self.logger.error(f"Error occurred during mission download request: {str(e)}", exc_info=True)
            self.emit_response(event_name, {'success': False, 'error': f'Internal bridge error: {str(e)}'})

    def _on_request_mission_upload(self, data):
        """Handles 'request_mission_upload' event from the server."""
        event_name = 'response_mission_upload'
        if not self.handler_mission_upload:
            self.logger.warning("Received 'request_mission_upload' but no handler configured.")
            self.emit_response(event_name, {'success': False, 'error': 'Bridge upload handler not configured'})
            return
        
        if not isinstance(data, list):
            self.emit_response(event_name, {'success': False, 'error': f'Invalid data format: Expected list'})
            return
        
        try:
            success = self.handler_mission_upload(data)
            if success: self.emit_response(event_name, {'success': True})
            else: self.emit_response(event_name, {'success': False, 'error': 'Mission upload failed on bridge/vehicle'})
        
        except Exception as e:
            self.logger.error(f"Error occurred during mission upload request: {str(e)}", exc_info=True)
            self.emit_response(event_name, {'success': False, 'error': f'Internal bridge error: {str(e)}'})

    def _on_request_start_operation(self, data):
        """Handles 'request_start_operation' event from the server."""
        event_name = 'response_start_operation'
        if not self.handler_start_operation:
            self.logger.warning("Received 'request_start_operation' but no handler configured.")
            self.emit_response(event_name, {'success': False, 'error': 'Bridge start_operation handler not configured'})
            return
        
        try:
            response_dict = self.handler_start_operation(data)
            if isinstance(response_dict, dict): self.emit_response(event_name, response_dict)
            else: self.logger.error("Operation start handler did not return a dictionary.")
        
        except Exception as e:
            self.logger.error(f"Error during start_operation request processing: {e}", exc_info=True)
            self.emit_response(event_name, {'success': False, 'error': f'Internal bridge error: {e}'})

    def _on_request_stop_operation(self, data):
        """Handles 'request_stop_operation' event from the server."""
        event_name = 'response_stop_operation'
        if not self.handler_stop_operation:
            self.logger.warning("Received 'request_stop_operation' but no handler configured.")
            self.emit_response(event_name, {'success': False, 'error': 'Bridge stop_operation handler not configured'})
            return
        
        try:
            response_dict = self.handler_stop_operation(data)
            if isinstance(response_dict, dict): self.emit_response(event_name, response_dict)
            else: self.logger.error("Operation stop handler did not return a dictionary.")
        
        except Exception as e:
            self.logger.error(f"Error during stop_operation request processing: {e}", exc_info=True)
            self.emit_response(event_name, {'success': False, 'error': f'Internal bridge error: {e}'})

    # --- Buffer Flushing ---
    def flush_buffer(self, buffer_manager) -> bool:
        """Emits buffered messages to the server via 'mavlink_message'."""
        if buffer_manager.is_empty() or not self.client.connected: return True
        try:
            buffer_content = buffer_manager.get_buffer_content()
            self.client.emit('mavlink_message', buffer_content)
            self.logger.info(f"Flushed {len(buffer_content)} MAVLink messages.")
            buffer_manager.clear_buffer()
            return True
        
        except Exception as e:
            self.logger.error(f"Socket.IO emit error during flush: {e}", exc_info=True)
            return False

    # --- Handler Setters ---
    def _update_handler(self, event_name: str, new_handler: Optional[Callable], internal_handler: Callable):
        """Generic helper to update a handler and re-bind the event."""
        try: self.client.off(event_name)
        except KeyError: pass
        
        attr_name = f"handler_{event_name.replace('request_', '')}"
        setattr(self, attr_name, new_handler)
        
        if new_handler:
            self.client.on(event_name, internal_handler)
            self.logger.info(f"Updated '{event_name}' handler.")
        
        else:
            self.logger.info(f"Removed '{event_name}' handler.")

    def set_handler_command(self, handler: Optional[Callable]):
        """Updates the handler for 'request_command' events."""
        self._update_handler('request_command', handler, self._on_request_command)

    def set_handler_mission_download(self, handler: Optional[Callable]):
        """Updates the handler for 'request_mission_download' events."""
        self._update_handler('request_mission_download', handler, self._on_request_mission_download)

    def set_handler_mission_upload(self, handler: Optional[Callable]):
        """Updates the handler for 'request_mission_upload' events."""
        self._update_handler('request_mission_upload', handler, self._on_request_mission_upload)

    def set_handler_start_operation(self, handler: Optional[Callable]):
        """Updates the handler for 'request_start_operation' events."""
        self._update_handler('request_start_operation', handler, self._on_request_start_operation)

    def set_handler_stop_operation(self, handler: Optional[Callable]):
        """Updates the handler for 'request_stop_operation' events."""
        self._update_handler('request_stop_operation', handler, self._on_request_stop_operation)

    def set_handlers(self, command=None, mission_download=None, mission_upload=None, start_operation=None, stop_operation=None):
        """Convenience method to set multiple handlers at once."""
        if command is not None: self.set_handler_command(command)
        if mission_download is not None: self.set_handler_mission_download(mission_download)
        if mission_upload is not None: self.set_handler_mission_upload(mission_upload)
        if start_operation is not None: self.set_handler_start_operation(start_operation)
        if stop_operation is not None: self.set_handler_stop_operation(stop_operation)
