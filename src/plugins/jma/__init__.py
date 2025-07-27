from ..import PluginRegistry
from .datasource import JMADataSource


def register_plugin(registry: PluginRegistry) -> None:
    registry.register('jma', JMADataSource)