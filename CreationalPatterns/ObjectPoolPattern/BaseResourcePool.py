import typing
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from queue import Queue, Empty
from typing import Optional

Resource = typing.TypeVar("Resource")


class BaseResourcePool:
    def __init__(self, max_connections: int = 4, timeout: float = 30.0):
        self.max_connections: int = max_connections
        self.timeout: float = timeout
        self.resource_pool: Queue[Resource] = Queue(maxsize=max_connections)
        self.threadpool = ThreadPoolExecutor(max_workers=4)

    @abstractmethod
    def init_object(self, *args, **kwargs):
        pass

    @contextmanager
    def acquire(self, timeout: Optional[float]):
        resource: Optional[Resource] = None
        try:
            resource = self._acquire_resource(timeout=timeout)
            yield resource
        except Empty as e:
            raise TimeoutError("Failed to acquire resource within given timeout")
        finally:
            if resource:
                self._release_resource(resource)

    def _acquire_resource(self, timeout: Optional[float]) -> Resource:
        if (timeout is None) or (timeout == 0):
            return self.resource_pool.get(block=False)
        return self.resource_pool.get(block=True, timeout=self.timeout)

    def _release_resource(self, resource: Resource):
        cleaned_up_resource: Resource = self.cleanup_resource(resource)
        self.resource_pool.put(resource)

    @staticmethod
    def cleanup_resource(resource: Resource) -> Resource:
        return resource
