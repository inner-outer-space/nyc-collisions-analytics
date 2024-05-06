import pandas as pd
import numpy as np  

import geopandas as gpd
from shapely.geometry import Point
import json

from astral import LocationInfo
from astral.sun import sun
from datetime import datetime, date
import pytz

def lon_lat_to_borough(lon, lat, polygon_df):
    try:
        point = Point(lon, lat)
        # Check if any polygon contains the point
        contains_point = polygon_df.contains(point)
        if contains_point.any():
            neighborhood = polygon_df.loc[contains_point, 'borough'].iloc[0]
            return neighborhood
        else:
            return 'None'
    except Exception as e:
        print(f"Error processing coordinates ({lon}, {lat}): {e}")
        return 'None'

def lon_lat_to_neighborhood(lon, lat, polygon_df):
    try:
        point = Point(lon, lat)
        # Check if any polygon contains the point
        contains_point = polygon_df.contains(point)
        if contains_point.any():
            neighborhood = polygon_df.loc[contains_point, 'neighborhood'].iloc[0]
            return neighborhood
        else:
            return 'None'
    except Exception as e:
        print(f"Error processing coordinates ({lon}, {lat}): {e}")
        return 'None'

def nyc_sunphase(datetime):
    """
    Determines the sun phase (day, dawn, dusk, or night) for a given date and time 
        in New York City using astral library.
        
    Parameters:
        date (datetime.date): The date used to determine sun phase.
        time (datetime.time): The time used to determine sun phase.

    Returns:
        str: The determined sun phase, which can be one of the following:
            - "day": The time is between sunrise and sunset.
            - "dawn": The time is between dawn and sunrise.
            - "dusk": The time is between sunset and dusk.
            - "night": The time is outside the above intervals.
    """
    date = datetime.date()
    time = datetime.time()  
    
    # Using Astral to determine sunrise, dusk, dawn, and sunset times
    city = LocationInfo("New York", "USA", "America/New_York", 40.730610, -73.935242)
    s = sun(city.observer, date=date, tzinfo=city.timezone)
    
    # Determine the sun phase
    if s['sunrise'].time() <= time <= s['sunset'].time():
        sun_phase = "day"
    elif s['dawn'].time() <= time <= s['sunrise'].time():
        sun_phase = "dawn"
    elif s['sunset'].time() <= time <= s['dusk'].time():
        sun_phase = "dusk"
    else:
        sun_phase = "night"
        
    return sun_phase