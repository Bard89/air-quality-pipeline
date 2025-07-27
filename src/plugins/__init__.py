from typing import Dict, Type
from ..domain.interfaces import DataSource
from importlib import import_module
import logging


logger = logging.getLogger(__name__)


class PluginRegistry:
    def __init__(self):
        self._plugins: Dict[str, Type[DataSource]] = {}

    def register(self, name: str, plugin_class: Type[DataSource]) -> None:
        if name in self._plugins:
            raise ValueError(f"Plugin '{name}' already registered")
        self._plugins[name] = plugin_class
        logger.info(f"Registered plugin: {name}")

    def get(self, name: str) -> Type[DataSource]:
        if name not in self._plugins:
            raise ValueError(f"Plugin '{name}' not found. Available: {list(self._plugins.keys())}")
        return self._plugins[name]

    def list_plugins(self) -> Dict[str, Type[DataSource]]:
        return self._plugins.copy()

    def auto_discover(self, module_path: str = "src.plugins") -> None:
        plugin_modules = [
            'openaq',
            'purpleair',
            'waqi',
            'jartic'
        ]
        
        for module_name in plugin_modules:
            try:
                module = import_module(f"{module_path}.{module_name}")
                if hasattr(module, 'register_plugin'):
                    module.register_plugin(self)
            except ImportError as e:
                logger.debug(f"Plugin module {module_name} not found: {e}")


_registry = PluginRegistry()


def get_registry() -> PluginRegistry:
    return _registry