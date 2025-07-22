import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


class DataStorage:
    def __init__(self, base_path: str = "data"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save_json(self, data: Dict, source: str, identifier: str) -> Path:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{identifier}_{timestamp}.json"
        
        dir_path = self.base_path / source / "raw"
        dir_path.mkdir(parents=True, exist_ok=True)
        
        filepath = dir_path / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        return filepath
    
    def get_processed_dir(self, source: str) -> Path:
        processed_dir = self.base_path / source / "processed"
        processed_dir.mkdir(parents=True, exist_ok=True)
        return processed_dir