from .datasource import OpenMeteoDataSource


def register_plugin(registry):
    registry.register('openmeteo', OpenMeteoDataSource)