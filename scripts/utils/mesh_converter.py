from typing import Tuple

def mesh_to_latlng(mesh_code: str) -> Tuple[float, float]:
    mesh_str = str(mesh_code)
    
    if len(mesh_str) == 6:
        lat_idx = int(mesh_str[:2])
        lon_idx = int(mesh_str[2:4])
        lat_sub = int(mesh_str[4])
        lon_sub = int(mesh_str[5])
        
        base_lat = lat_idx * 2/3
        base_lon = lon_idx + 100
        
        sub_lat = lat_sub * (2/3) / 8
        sub_lon = lon_sub * 1 / 8
        
        lat = base_lat + sub_lat + (2/3) / 16
        lon = base_lon + sub_lon + 1 / 16
        
        return (lat, lon)
    
    elif len(mesh_str) == 8:
        primary_code = mesh_str[:6]
        secondary_digit = int(mesh_str[6:8])
        
        base_lat, base_lon = mesh_to_latlng(primary_code)
        
        row = (secondary_digit - 1) // 10
        col = (secondary_digit - 1) % 10
        
        secondary_lat_offset = row * (2/3) / 80 - (2/3) / 16 + (2/3) / 160
        secondary_lon_offset = col * 1 / 80 - 1 / 16 + 1 / 160
        
        lat = base_lat + secondary_lat_offset
        lon = base_lon + secondary_lon_offset
        
        return (lat, lon)
    
    else:
        raise ValueError(f"Unsupported mesh code length: {len(mesh_str)}")