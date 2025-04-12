import threading

class ThreadSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            return self._dict.get(key)

    def put(self, key, value):
        with self._lock:
            self._dict[key] = value

    def remove(self, key):
        with self._lock:
            self._dict.pop(key, None)

    def __contains__(self, key):
        with self._lock:
            return key in self._dict

    def __getitem__(self, key):
        with self._lock:
            return self._dict[key]

    def __setitem__(self, key, value):
        with self._lock:
            self._dict[key] = value