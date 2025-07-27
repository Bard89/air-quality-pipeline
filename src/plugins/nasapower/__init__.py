from ..import PluginRegistry
from .datasource import NASAPowerDataSource


def register_plugin(registry: PluginRegistry) -> None:
    registry.register('nasapower', NASAPowerDataSource)