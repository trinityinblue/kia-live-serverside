import time
from threading import RLock
from queue import PriorityQueue
from google.transit import gtfs_realtime_pb2


class ThreadSafeDict:
    def __init__(self):
        self._data = {}
        self._lock = RLock()

    def __iter__(self):
        with self._lock:
            return iter(self._data.items())

    def __len__(self):
        with self._lock:
            return len(self._data)

    def get(self, key, default=None):
        with self._lock:
            return self._data.get(key, default)

    def items(self):
        with self._lock:
            return list(self._data.items())

    def keys(self):
        with self._lock:
            return list(self._data.keys())

    def values(self):
        with self._lock:
            return list(self._data.values())

    def __getitem__(self, key):
        with self._lock:
            return self._data[key]

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = value

    def clear(self):
        with self._lock:
            self._data.clear()

    def update(self, new_data):
        with self._lock:
            self._data.update(new_data)

    def as_dict(self):
        with self._lock:
            return self._data.copy()

    def pop(self, key, default=None):
        with self._lock:
            return self._data.pop(key, default)

    def __contains__(self, key):
        with self._lock:
            return key in self._data


# Thread-safe data stores
scheduled_timings = PriorityQueue()
feed_message = gtfs_realtime_pb2.FeedMessage()
feed_message_lock = RLock()
with feed_message_lock:
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(time.time())

# Thread-safe shared dicts
routes_children = ThreadSafeDict()
routes_parent = ThreadSafeDict()
start_times = ThreadSafeDict()
