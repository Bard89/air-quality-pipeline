from .datasource import OpenAQDataSource
from ..import PluginRegistry


def register_plugin(registry: PluginRegistry) -> None:
    registry.register('openaq', OpenAQDataSource)