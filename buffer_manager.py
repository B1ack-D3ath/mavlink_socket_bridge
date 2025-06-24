import time
import logging

class BufferManager:
    """
    Handles buffering of MAVLink message dictionaries and checks flush conditions.
    Assumes messages are pre-cleaned (e.g., NaN replaced with None).
    """

    def __init__(self, buffer_size=50, flush_timeout=2.0, logger=None):
        """
        Initializes the message buffer.

        Args:
            buffer_size (int): Max number of messages before indicating buffer is full.
            flush_timeout (float): Seconds of inactivity before indicating timeout.
            logger (logging.Logger): Logger instance.
        """
        self.buffer_size = buffer_size
        self.flush_timeout = flush_timeout
        self.buffer = [] # Holds pre-cleaned message dictionaries
        self.last_msg_time = time.time()
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info(f"BufferManager initialized: size={buffer_size}, timeout={flush_timeout}s")

    def add_message(self, msg_dict):
        """
        Adds a pre-cleaned MAVLink message dictionary to the buffer.

        Args:
            msg_dict (dict): The pre-cleaned MAVLink message dictionary.

        Returns:
            bool: True if the buffer reached capacity after adding, False otherwise.
        """
        try:
            self.buffer.append(msg_dict)
            self.last_msg_time = time.time()
            if len(self.buffer) > self.buffer_size:
                self.buffer.pop(0)  # Remove the oldest message if buffer exceeds size
            
            self.logger.debug(f"Buffer size: {len(self.buffer)}/{self.buffer_size}") # Removed DEBUG log
            return len(self.buffer) >= self.buffer_size
        except Exception as e:
            # Removed print statement
            self.logger.error(f"BufferManager error adding message dict: {str(e)}", exc_info=True)
            return False

    def check_timeout(self):
        """Checks if the inactivity timeout has been reached while buffer has messages."""
        is_timed_out = (time.time() - self.last_msg_time) > self.flush_timeout
        has_messages = bool(self.buffer)
        return is_timed_out and has_messages

    def clear_buffer(self):
        """Clears the message buffer and resets the last message timestamp."""
        self.buffer.clear()
        self.last_msg_time = time.time()
        # self.logger.debug("Buffer cleared") # Removed DEBUG log

    def get_buffer_content(self):
        """Returns the current list of buffered message dictionaries."""
        return self.buffer

    def is_empty(self):
        """Checks if the buffer is currently empty."""
        return not self.buffer
