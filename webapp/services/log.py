import threading

class ScrapingLog:
    logs = []  # Shared log storage
    _lock = threading.Lock()  # Thread-safe lock

    @classmethod
    def add_log(cls, message):
        """Adds a log message to the log list in a thread-safe manner."""
        with cls._lock:
            cls.logs.append(message)
            print(f"Added log: {message}")

    @classmethod
    def get_logs(cls):
        """Returns all stored logs in a thread-safe manner."""
        with cls._lock:
            return cls.logs.copy()  # Return a copy to prevent modification

    @classmethod
    def clear_logs(cls):
        """Clears the stored logs in a thread-safe manner."""
        with cls._lock:
            cls.logs.clear()
