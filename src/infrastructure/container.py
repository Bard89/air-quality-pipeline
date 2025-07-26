from typing import Dict, Type, Any, Optional, TypeVar, Generic, Callable
from functools import wraps
import inspect
from dataclasses import dataclass, field
from ..domain.interfaces import DataSource, Storage, RateLimiter, Cache, JobManager, MetricsCollector


T = TypeVar('T')


@dataclass
class ServiceDescriptor(Generic[T]):
    factory: Callable[..., T]
    singleton: bool = False
    instance: Optional[T] = None


class Container:
    def __init__(self):
        self._services: Dict[Type, ServiceDescriptor] = {}
        self._config: Dict[str, Any] = {}

    def register(
        self, 
        interface: Type[T], 
        factory: Callable[..., T], 
        singleton: bool = False
    ) -> None:
        self._services[interface] = ServiceDescriptor(
            factory=factory,
            singleton=singleton
        )

    def register_singleton(self, interface: Type[T], instance: T) -> None:
        self._services[interface] = ServiceDescriptor(
            factory=lambda: instance,
            singleton=True,
            instance=instance
        )

    def resolve(self, interface: Type[T]) -> T:
        if interface not in self._services:
            raise ValueError(f"No registration found for {interface}")

        descriptor = self._services[interface]

        if descriptor.singleton and descriptor.instance is not None:
            return descriptor.instance

        sig = inspect.signature(descriptor.factory)
        kwargs = {}

        for param_name, param in sig.parameters.items():
            if param.annotation != inspect.Parameter.empty:
                if param.annotation in self._services:
                    kwargs[param_name] = self.resolve(param.annotation)
                elif param_name in self._config:
                    kwargs[param_name] = self._config[param_name]

        instance = descriptor.factory(**kwargs)

        if descriptor.singleton:
            descriptor.instance = instance

        return instance

    def set_config(self, key: str, value: Any) -> None:
        self._config[key] = value

    def get_config(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)


def inject(container: Container):
    def decorator(func):
        sig = inspect.signature(func)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            for param_name, param in sig.parameters.items():
                if param_name not in kwargs and param.annotation != inspect.Parameter.empty:
                    if param.annotation in container._services:
                        kwargs[param_name] = container.resolve(param.annotation)
            
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


_container = Container()


def get_container() -> Container:
    return _container