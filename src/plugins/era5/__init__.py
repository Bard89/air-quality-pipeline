from ..import PluginRegistry
from .datasource import ERA5DataSource


def register_plugin(registry: PluginRegistry) -> None:
    registry.register('era5', ERA5DataSource)