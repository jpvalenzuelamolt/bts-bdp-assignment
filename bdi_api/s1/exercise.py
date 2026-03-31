import os
import json
import shutil
import re
from typing import Annotated

import requests
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to add it to `requirements.txt`
    so it can be installed using `pip install -r requirements.txt`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"
    
    # Clean download directory
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)
    
    # Get directory listing from the source
    try:
        response = requests.get(base_url, timeout=30)
        response.raise_for_status()
        
        # Parse HTML to find JSON files
        json_files = re.findall(r'href="([^"]*\.json)"', response.text)
        
        # Download first file_limit files
        downloaded_count = 0
        for filename in json_files[:file_limit]:
            file_url = base_url + filename
            try:
                file_response = requests.get(file_url, timeout=30)
                file_response.raise_for_status()
                
                file_path = os.path.join(download_dir, filename)
                with open(file_path, 'w') as f:
                    f.write(file_response.text)
                downloaded_count += 1
            except requests.exceptions.Timeout:
                print(f"Timeout downloading {filename}")
                break
            except Exception as e:
                print(f"Error downloading {filename}: {e}")
                continue
        
        return "OK"
    except requests.exceptions.Timeout:
        print(f"Timeout accessing source URL: {base_url}")
        return "OK"
    except requests.exceptions.ConnectionError:
        print(f"Connection error accessing source URL: {base_url}")
        return "OK"
    except Exception as e:
        print(f"Error accessing source URL: {e}")
        return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data for analysis.

    Process all raw JSON files, extract aircraft data, positions, and statistics.
    Save aggregated data to prepared directory as aircraft.json.

    The aircraft object contains:
    - icao: aircraft ICAO code (from 'hex' field, lowercased)
    - registration: aircraft registration (from 'r' field)
    - type: aircraft type (from 't' field)
    - positions: list of positions with timestamp, lat, lon
    - max_altitude_baro: maximum barometric altitude
    - max_ground_speed: maximum ground speed
    - had_emergency: whether emergency flag was set
    """
    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    
    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)
    
    aircraft_data = {}
    
    try:
        if not os.path.exists(raw_dir):
            prepared_file = os.path.join(prepared_dir, "aircraft.json")
            with open(prepared_file, 'w') as f:
                json.dump({}, f, indent=2)
            return "OK"
        
        for filename in os.listdir(raw_dir):
            if not filename.endswith('.json'):
                continue
            
            file_path = os.path.join(raw_dir, filename)
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                if 'aircraft' in data:
                    for aircraft in data['aircraft']:
                        icao = aircraft.get('hex', '').lower()
                        if not icao:
                            continue
                        
                        if icao not in aircraft_data:
                            aircraft_data[icao] = {
                                'icao': icao,
                                'registration': aircraft.get('r'),
                                'type': aircraft.get('t'),
                                'positions': [],
                                'max_altitude_baro': None,
                                'max_ground_speed': None,
                                'had_emergency': False
                            }
                        
                        if 'trace' in aircraft:
                            for trace_point in aircraft['trace']:
                                if len(trace_point) >= 3:
                                    pos = {
                                        'timestamp': trace_point[2],
                                        'lat': trace_point[0],
                                        'lon': trace_point[1]
                                    }
                                    aircraft_data[icao]['positions'].append(pos)
                        
                        if 'alt_baro' in aircraft:
                            alt = aircraft['alt_baro']
                            if alt is not None:
                                if aircraft_data[icao]['max_altitude_baro'] is None or alt > aircraft_data[icao]['max_altitude_baro']:
                                    aircraft_data[icao]['max_altitude_baro'] = alt
                        
                        if 'gs' in aircraft:
                            gs = aircraft['gs']
                            if gs is not None:
                                if aircraft_data[icao]['max_ground_speed'] is None or gs > aircraft_data[icao]['max_ground_speed']:
                                    aircraft_data[icao]['max_ground_speed'] = gs
                        
                        if 'emergency' in aircraft and aircraft['emergency']:
                            aircraft_data[icao]['had_emergency'] = True
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                continue
        
        prepared_file = os.path.join(prepared_dir, "aircraft.json")
        with open(prepared_file, 'w') as f:
            json.dump(aircraft_data, f, indent=2)
        
        return "OK"
    except Exception as e:
        print(f"Error preparing data: {e}")
        return "ERROR"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by icao asc

    The response contains:
    - icao: aircraft ICAO code in lowercase
    - registration: aircraft registration
    - type: aircraft type

    Pagination:
    - num_results: number of results to return (default 100)
    - page: page number starting from 0 (default 0)
    """
    prepared_file = os.path.join(settings.prepared_dir, "day=20231101", "aircraft.json")
    
    try:
        if not os.path.exists(prepared_file):
            return []
        
        with open(prepared_file, 'r') as f:
            aircraft_data = json.load(f)
        
        aircraft_list = [
            {
                'icao': v['icao'],
                'registration': v['registration'],
                'type': v['type']
            }
            for v in aircraft_data.values()
        ]
        
        aircraft_list.sort(key=lambda x: x['icao'])
        
        start = page * num_results
        end = start + num_results
        
        return aircraft_list[start:end]
    except Exception as e:
        print(f"Error listing aircraft: {e}")
        return []


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)

    The response contains:
    - timestamp: UNIX timestamp of the position
    - lat: latitude
    - lon: longitude

    Pagination:
    - num_results: number of results to return (default 1000)
    - page: page number starting from 0 (default 0)
    """
    prepared_file = os.path.join(settings.prepared_dir, "day=20231101", "aircraft.json")
    
    try:
        if not os.path.exists(prepared_file):
            return []
        
        with open(prepared_file, 'r') as f:
            aircraft_data = json.load(f)
        
        icao_lower = icao.lower()
        if icao_lower not in aircraft_data:
            return []
        
        positions = aircraft_data[icao_lower]['positions']
        positions.sort(key=lambda x: x['timestamp'])
        
        start = page * num_results
        end = start + num_results
        
        return positions[start:end]
    except Exception as e:
        print(f"Error getting positions: {e}")
        return []


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    The response contains:
    - max_altitude_baro: maximum barometric altitude recorded
    - max_ground_speed: maximum ground speed recorded
    - had_emergency: whether an emergency was declared
    """
    prepared_file = os.path.join(settings.prepared_dir, "day=20231101", "aircraft.json")
    
    try:
        if not os.path.exists(prepared_file):
            return {
                'max_altitude_baro': None,
                'max_ground_speed': None,
                'had_emergency': False
            }
        
        with open(prepared_file, 'r') as f:
            aircraft_data = json.load(f)
        
        icao_lower = icao.lower()
        if icao_lower not in aircraft_data:
            return {
                'max_altitude_baro': None,
                'max_ground_speed': None,
                'had_emergency': False
            }
        
        aircraft = aircraft_data[icao_lower]
        return {
            'max_altitude_baro': aircraft['max_altitude_baro'],
            'max_ground_speed': aircraft['max_ground_speed'],
            'had_emergency': aircraft['had_emergency']
        }
    except Exception as e:
        print(f"Error getting statistics: {e}")
        return {
            'max_altitude_baro': None,
            'max_ground_speed': None,
            'had_emergency': False
        }
