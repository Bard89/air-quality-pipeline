import pytest
from decimal import Decimal
from datetime import datetime
from src.domain.models import (
    Coordinates, Location, Sensor, Measurement,
    ParameterType, MeasurementUnit
)


class TestCoordinates:
    def test_valid_coordinates(self):
        coords = Coordinates(latitude=Decimal("40.7128"), longitude=Decimal("-74.0060"))
        assert coords.latitude == Decimal("40.7128")
        assert coords.longitude == Decimal("-74.0060")

    def test_invalid_latitude(self):
        with pytest.raises(ValueError, match="Invalid latitude"):
            Coordinates(latitude=Decimal("91"), longitude=Decimal("0"))

    def test_invalid_longitude(self):
        with pytest.raises(ValueError, match="Invalid longitude"):
            Coordinates(latitude=Decimal("0"), longitude=Decimal("181"))


class TestLocation:
    def test_location_creation(self):
        coords = Coordinates(latitude=Decimal("35.6762"), longitude=Decimal("139.6503"))
        location = Location(
            id="123",
            name="Tokyo Station",
            coordinates=coords,
            city="Tokyo",
            country="JP"
        )
        
        assert location.id == "123"
        assert location.name == "Tokyo Station"
        assert location.city == "Tokyo"
        assert location.country == "JP"

    def test_location_hashable(self):
        coords = Coordinates(latitude=Decimal("0"), longitude=Decimal("0"))
        loc1 = Location(id="1", name="Test", coordinates=coords)
        loc2 = Location(id="1", name="Different", coordinates=coords)
        
        assert hash(loc1) == hash(loc2)
        
        locations_set = {loc1, loc2}
        assert len(locations_set) == 1


class TestSensor:
    def test_sensor_creation(self):
        coords = Coordinates(latitude=Decimal("0"), longitude=Decimal("0"))
        location = Location(id="loc1", name="Test Location", coordinates=coords)
        
        sensor = Sensor(
            id="sensor1",
            location=location,
            parameter=ParameterType.PM25,
            unit=MeasurementUnit.MICROGRAMS_PER_CUBIC_METER
        )
        
        assert sensor.id == "sensor1"
        assert sensor.parameter == ParameterType.PM25
        assert sensor.is_active is True


class TestMeasurement:
    def test_valid_measurement(self):
        coords = Coordinates(latitude=Decimal("0"), longitude=Decimal("0"))
        location = Location(id="loc1", name="Test", coordinates=coords)
        sensor = Sensor(
            id="s1",
            location=location,
            parameter=ParameterType.PM25,
            unit=MeasurementUnit.MICROGRAMS_PER_CUBIC_METER
        )
        
        measurement = Measurement(
            sensor=sensor,
            timestamp=datetime.utcnow(),
            value=Decimal("25.5")
        )
        
        assert measurement.value == Decimal("25.5")

    def test_negative_value_for_non_temperature(self):
        coords = Coordinates(latitude=Decimal("0"), longitude=Decimal("0"))
        location = Location(id="loc1", name="Test", coordinates=coords)
        sensor = Sensor(
            id="s1",
            location=location,
            parameter=ParameterType.PM25,
            unit=MeasurementUnit.MICROGRAMS_PER_CUBIC_METER
        )
        
        with pytest.raises(ValueError, match="Negative value"):
            Measurement(
                sensor=sensor,
                timestamp=datetime.utcnow(),
                value=Decimal("-5")
            )

    def test_negative_temperature_allowed(self):
        coords = Coordinates(latitude=Decimal("0"), longitude=Decimal("0"))
        location = Location(id="loc1", name="Test", coordinates=coords)
        sensor = Sensor(
            id="s1",
            location=location,
            parameter=ParameterType.TEMPERATURE,
            unit=MeasurementUnit.CELSIUS
        )
        
        measurement = Measurement(
            sensor=sensor,
            timestamp=datetime.utcnow(),
            value=Decimal("-10")
        )
        
        assert measurement.value == Decimal("-10")