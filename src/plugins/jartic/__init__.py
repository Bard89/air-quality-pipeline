from ..import PluginRegistry
from .datasource import JARTICDataSource


def register_plugin(registry: PluginRegistry) -> None:
    registry.register('jartic', JARTICDataSource)