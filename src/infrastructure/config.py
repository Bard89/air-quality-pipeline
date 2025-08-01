from pydantic import BaseModel, Field, validator, root_validator
from typing import List, Dict, Optional, Any
from pathlib import Path
import os
import json
import yaml
from dotenv import load_dotenv


class APIConfig(BaseModel):
    base_url: str
    rate_limit: int = Field(default=60, ge=1)
    timeout: int = Field(default=30, ge=1)
    retry_count: int = Field(default=3, ge=0)
    retry_delay: float = Field(default=1.0, ge=0)


class StorageConfig(BaseModel):
    base_path: Path = Field(default=Path("data"))
    checkpoint_dir: Path = Field(default=Path("data/checkpoints"))
    batch_size: int = Field(default=1000, ge=1)
    file_format: str = Field(default="csv", regex="^(csv|parquet|json)$")

    @validator('base_path', 'checkpoint_dir', pre=True)
    def convert_to_path(cls, v):
        return Path(v) if isinstance(v, str) else v


class CacheConfig(BaseModel):
    type: str = Field(default="memory", regex="^(memory|redis|disk)$")
    ttl: int = Field(default=300, ge=0)
    max_size: int = Field(default=1000, ge=1)
    redis_url: Optional[str] = None


class LoggingConfig(BaseModel):
    level: str = Field(default="INFO", regex="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    format: str = Field(default="json", regex="^(json|text)$")
    file_path: Optional[Path] = None
    rotate_size: int = Field(default=10_000_000, ge=0)
    backup_count: int = Field(default=5, ge=0)


class MetricsConfig(BaseModel):
    enabled: bool = Field(default=True)
    type: str = Field(default="prometheus", regex="^(prometheus|statsd|cloudwatch)$")
    namespace: str = Field(default="airquality")
    port: int = Field(default=9090, ge=1, le=65535)
    flush_interval: int = Field(default=10, ge=1)


class DataSourceConfig(BaseModel):
    name: str
    type: str
    enabled: bool = Field(default=True)
    api_keys: List[str] = Field(default_factory=list)
    config: Dict[str, Any] = Field(default_factory=dict)

    @validator('api_keys', pre=True)
    def load_api_keys_from_env(cls, v, values):
        """
        Load API keys from environment variables.
        
        Supports two formats:
        - Single key: {NAME}_API_KEY
        - Multiple keys: {NAME}_API_KEY_01, {NAME}_API_KEY_02, ..., {NAME}_API_KEY_99
        
        Note: Numbered keys MUST use zero-padded two-digit format (01, 02, etc.)
        """
        if not v and 'name' in values:
            name_upper = values['name'].upper()
            
            # First check for single key
            single_key = os.getenv(f"{name_upper}_API_KEY")
            if single_key:
                return [single_key]
            
            # Then check for numbered keys with zero-padded format
            keys = []
            prefix = f"{name_upper}_API_KEY_"
            for env_key, env_value in os.environ.items():
                if env_key.startswith(prefix):
                    suffix = env_key[len(prefix):]
                    # Enforce zero-padded two-digit format
                    if len(suffix) == 2 and suffix.isdigit():
                        key_num = int(suffix)
                        if 1 <= key_num <= 99:
                            keys.append((suffix, env_value))  # Keep suffix for stable sort
            
            # Sort by suffix string (maintains zero-padded order)
            if keys:
                keys.sort(key=lambda x: x[0])
                return [key[1] for key in keys]
            
            return []
        return v


class ApplicationConfig(BaseModel):
    app_name: str = Field(default="air-quality-collector")
    environment: str = Field(default="production", regex="^(development|staging|production)$")
    debug: bool = Field(default=False)
    
    storage: StorageConfig = Field(default_factory=StorageConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    
    data_sources: List[DataSourceConfig] = Field(default_factory=list)
    
    parallel_downloads: bool = Field(default=True)
    max_concurrent_locations: int = Field(default=5, ge=1)
    max_concurrent_sensors: int = Field(default=10, ge=1)

    class Config:
        validate_assignment = True
        use_enum_values = True

    @root_validator
    def validate_config(cls, values):
        env = values.get('environment')
        if env == 'development':
            values['debug'] = True
            if 'logging' in values:
                values['logging'].level = 'DEBUG'
        return values

    @classmethod
    def from_file(cls, config_path: Path) -> 'ApplicationConfig':
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path) as f:
            if config_path.suffix == '.json':
                data = json.load(f)
            elif config_path.suffix in ['.yml', '.yaml']:
                data = yaml.safe_load(f)
            else:
                raise ValueError(f"Unsupported config format: {config_path.suffix}")
        
        return cls(**data)

    @classmethod
    def from_env(cls) -> 'ApplicationConfig':
        load_dotenv()
        
        config = cls()
        
        # Get available data sources from environment or use defaults
        available_sources = os.getenv('AVAILABLE_DATA_SOURCES', 'openaq,purpleair,waqi').split(',')
        
        config.data_sources = []
        for source_name in available_sources:
            source_name = source_name.strip()
            source_config = DataSourceConfig(
                name=source_name,
                type=source_name,
                enabled=os.getenv(f"{source_name.upper()}_ENABLED", "true").lower() == "true"
            )
            if source_config.api_keys:
                config.data_sources.append(source_config)
        
        if env := os.getenv('ENVIRONMENT'):
            config.environment = env
        
        if debug := os.getenv('DEBUG'):
            config.debug = debug.lower() == 'true'
        
        return config