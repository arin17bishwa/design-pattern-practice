from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from queue import Queue, Empty
from typing import Optional, TypeVar

Resource = TypeVar("Resource")


class BaseResourcePool:
    def __init__(self, max_resources: int = 8, resource_creation_kwargs=None):
        if resource_creation_kwargs is None:
            resource_creation_kwargs = {}
        self.max_resources: int = max_resources
        self.resource_creation_kwargs = resource_creation_kwargs  # an attribute so we can recreate resources if needed
        self.resource_pool: Queue[Resource] = Queue(maxsize=max_resources)
        self.threadpool_executor = ThreadPoolExecutor(max_workers=4)
        self.init_resource_pool()

    def init_resource_pool(self, *args, **kwargs):
        with self.threadpool_executor as executor:
            futures = [
                executor.submit(self.init_resource, **self.resource_creation_kwargs)
                for _ in range(self.max_resources)
            ]
        for completed_future in as_completed(futures):
            self.resource_pool.put(completed_future.result())

    @abstractmethod
    def init_resource(self, **kwargs) -> Resource: ...

    @contextmanager
    def acquire(self, timeout: Optional[float] = None) -> Resource:
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
        return self.resource_pool.get(block=True, timeout=timeout)

    def _release_resource(self, resource: Resource):
        cleaned_up_resource: Resource = self.cleanup_resource(resource)
        self.resource_pool.put(resource)

    @staticmethod
    def cleanup_resource(resource: Resource) -> Resource:
        return resource
