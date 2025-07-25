import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

class CheckpointManager:
    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = self.checkpoint_dir / "checkpoint_history.json"
        self.history = self._load_history()
    
    def _load_history(self) -> Dict[str, List[Dict]]:
        """Load checkpoint history from file"""
        if self.history_file.exists():
            with open(self.history_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_history(self):
        """Save checkpoint history to file"""
        with open(self.history_file, 'w') as f:
            json.dump(self.history, f, indent=2)
    
    def save_checkpoint(self, country_code: str, location_index: int, total_locations: int,
                       completed_locations: List[int], output_file: str, 
                       current_location_id: Optional[int] = None) -> str:
        """Save checkpoint and add to history"""
        checkpoint_data = {
            "country_code": country_code,
            "location_index": location_index,
            "total_locations": total_locations,
            "completed_locations": completed_locations,
            "output_file": output_file,
            "current_location_id": current_location_id,
            "timestamp": datetime.now().isoformat()
        }
        
        # Save current checkpoint
        checkpoint_file = self.checkpoint_dir / f"checkpoint_{country_code.lower()}_all_parallel.json"
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
        
        # Add to history
        output_key = output_file
        if output_key not in self.history:
            self.history[output_key] = []
        
        # Add checkpoint to history
        self.history[output_key].append({
            "checkpoint_file": str(checkpoint_file),
            "location_index": location_index,
            "timestamp": checkpoint_data["timestamp"],
            "total_locations": total_locations
        })
        
        self._save_history()
        return str(checkpoint_file)
    
    def find_checkpoint_for_file(self, output_file: str) -> Optional[Dict]:
        """Find the latest checkpoint for a given output file"""
        if output_file in self.history:
            checkpoints = self.history[output_file]
            if checkpoints:
                # Return the latest checkpoint
                latest = max(checkpoints, key=lambda x: x["timestamp"])
                checkpoint_path = Path(latest["checkpoint_file"])
                if checkpoint_path.exists():
                    with open(checkpoint_path, 'r') as f:
                        return json.load(f)
        return None
    
    def get_or_create_output_file(self, country_code: str, resume: bool = True) -> Tuple[Path, Optional[Dict]]:
        """Get existing output file or create new one"""
        if resume:
            # Look for existing checkpoints for this country
            for output_file, checkpoints in self.history.items():
                if checkpoints and country_code.lower() in output_file.lower():
                    # Check if file exists
                    output_path = Path(output_file)
                    if output_path.exists():
                        # Load the checkpoint
                        checkpoint = self.find_checkpoint_for_file(output_file)
                        if checkpoint and checkpoint['country_code'] == country_code:
                            print(f"\nFound existing download: {output_file}")
                            print(f"Last checkpoint: location {checkpoint['location_index']}/{checkpoint['total_locations']}")
                            return output_path, checkpoint
        
        # Create new output file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{country_code.lower()}_airquality_all_{timestamp}.csv"
        output_path = Path(f'data/openaq/processed/{filename}')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        return output_path, None
    
    def list_downloads(self, country_code: Optional[str] = None) -> List[Dict]:
        """List all downloads with their status"""
        downloads = []
        for output_file, checkpoints in self.history.items():
            if checkpoints:
                latest = max(checkpoints, key=lambda x: x["timestamp"])
                # Filter by country if specified
                if country_code and country_code.lower() not in output_file.lower():
                    continue
                    
                # Check if file exists
                file_exists = Path(output_file).exists()
                file_size = Path(output_file).stat().st_size if file_exists else 0
                
                downloads.append({
                    "output_file": output_file,
                    "exists": file_exists,
                    "size_mb": file_size / 1024 / 1024,
                    "last_location": latest["location_index"],
                    "total_locations": latest["total_locations"],
                    "progress_pct": (latest["location_index"] / latest["total_locations"] * 100) if latest["total_locations"] > 0 else 0,
                    "last_update": latest["timestamp"],
                    "checkpoints": len(checkpoints)
                })
        
        return sorted(downloads, key=lambda x: x["last_update"], reverse=True)