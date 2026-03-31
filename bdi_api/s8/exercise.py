from fastapi import APIRouter, status
from pydantic import BaseModel
import json
from pathlib import Path
import pandas as pd

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending.

    The data should come from the silver layer (processed by the Airflow DAG).
    Paginated with `num_results` per page and `page` number (0-indexed).
    """
    silver_path = Path(settings.prepared_dir) / "aircraft_enriched.parquet"
    json_path = Path(settings.prepared_dir) / "aircraft_enriched.json"
    
    try:
        if silver_path.exists():
            df = pd.read_parquet(silver_path)
        elif json_path.exists():
            df = pd.read_json(json_path)
        else:
            parquet_files = list(Path(settings.prepared_dir).glob("*.parquet"))
            if parquet_files:
                df = pd.read_parquet(parquet_files[0])
            else:
                return []
        
        df = df.rename(columns={
            'hex': 'icao',
            'r': 'registration',
            't': 'type'
        }, errors='ignore')
        
        df = df.sort_values('icao', ascending=True)
        
        start = page * num_results
        end = start + num_results
        df_page = df.iloc[start:end]
        
        result = []
        for _, row in df_page.iterrows():
            result.append(AircraftReturn(
                icao=str(row.get('icao', '')),
                registration=row.get('registration'),
                type=row.get('type'),
                owner=row.get('owner'),
                manufacturer=row.get('manufacturer'),
                model=row.get('model')
            ))
        
        return result
    except Exception as e:
        print(f"Error reading aircraft data: {e}")
        return []


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day.

    Computation:
    - Each row in the tracking data represents a 5-second observation
    - hours_flown = (number_of_observations * 5) / 3600
    - Look up `galph` (gallons per hour) from fuel consumption rates using the aircraft's ICAO type
    - fuel_used_kg = hours_flown * galph * 3.04
    - co2_tons = (fuel_used_kg * 3.15) / 907.185
    - If fuel consumption rate is not available for this aircraft type, return None for co2
    """
    try:
        fuel_rates = {}
        fuel_rates_path = Path(settings.prepared_dir) / "fuel_consumption_rates.json"
        if fuel_rates_path.exists():
            with open(fuel_rates_path, 'r') as f:
                fuel_rates = json.load(f)
        
        day_parts = day.split('-')
        if len(day_parts) == 3:
            year, month, day_num = day_parts
        else:
            year, month, day_num = '2023', '11', '01'
        
        tracking_path = Path(settings.prepared_dir) / year / month / day_num / "tracking_data.parquet"
        count_observations = 0
        
        if tracking_path.exists():
            try:
                df = pd.read_parquet(tracking_path)
                df_aircraft = df[df['hex'].astype(str).str.upper() == icao.upper()]
                count_observations = len(df_aircraft)
            except Exception as e:
                print(f"Error reading tracking data: {e}")
        else:
            alt_paths = [
                Path(settings.prepared_dir) / f"{year}{month}{day_num}.parquet",
                Path(settings.prepared_dir) / "tracking_data.parquet",
            ]
            for alt_path in alt_paths:
                if alt_path.exists():
                    try:
                        df = pd.read_parquet(alt_path)
                        df_aircraft = df[df['hex'].astype(str).str.upper() == icao.upper()]
                        count_observations = len(df_aircraft)
                        break
                    except:
                        continue
        
        hours_flown = (count_observations * 5) / 3600
        
        aircraft_type = None
        silver_path = Path(settings.prepared_dir) / "aircraft_enriched.parquet"
        if silver_path.exists():
            try:
                df = pd.read_parquet(silver_path)
                aircraft = df[df['hex'].astype(str).str.upper() == icao.upper()]
                if not aircraft.empty:
                    aircraft_type = aircraft.iloc[0].get('t') or aircraft.iloc[0].get('type')
            except:
                pass
        
        co2_value = None
        if aircraft_type and aircraft_type in fuel_rates:
            galph = fuel_rates[aircraft_type].get('galph', fuel_rates[aircraft_type].get('gallons_per_hour'))
            if galph:
                fuel_used_kg = hours_flown * galph * 3.04
                co2_tons = (fuel_used_kg * 3.15) / 907.185
                co2_value = co2_tons
        
        return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2_value)
    except Exception as e:
        print(f"Error calculating CO2: {e}")
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)
