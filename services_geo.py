import urllib.parse

import requests
import streamlit as st

from core import get_logger

@st.cache_data(ttl=3600) # Cache for 1 hour
def get_lat_lon_from_address(address):
    """Uses Open-Meteo Geocoding API to geocode an address to Lat/Lon.
       Falls back to city search if full address fails.
    """
    try:
        # Helper to query Open-Meteo
        def query_open_meteo(query):
            if not query or not query.strip(): return {}
            encoded_query = urllib.parse.quote(query.strip())
            url = f"https://geocoding-api.open-meteo.com/v1/search?name={encoded_query}&count=1&language=en&format=json"
            headers = {'User-Agent': '5GSecurityJobBoard/1.0'}
            try:
                response = requests.get(url, headers=headers, timeout=5)
                return response.json()
            except:
                return {}

        # 1. Try full address (unlikely to work for streets, but good for "City, State")
        data = query_open_meteo(address)
        
        if 'results' in data and data['results']:
            result = data['results'][0]
            return result.get('latitude'), result.get('longitude')
        
        # 2. Fallback: Try to extract City from "Street, City, State" format
        parts = [p.strip() for p in address.split(',')]
        if len(parts) >= 2:
            # Heuristic:
            # If 3+ parts (e.g. "Street, City, State, Country"), City is likely index 1.
            # If 2 parts (e.g. "City, State"), City is likely index 0.
            potential_city = parts[1] if len(parts) >= 3 else parts[0]
            
            # Avoid searching for things that look like states or zip codes if possible, 
            # but Open-Meteo is robust.
            data = query_open_meteo(potential_city)
            if 'results' in data and data['results']:
                result = data['results'][0]
                return result.get('latitude'), result.get('longitude')

        get_logger().log(f"Geocoding failed for '{address}': No results found from Open-Meteo")
        return None, None
            
    except Exception as e:
        get_logger().log(f"Geocoding failed for '{address}': {e}")
        return None, None

@st.cache_data(ttl=1800) # Cache for 30 mins (weather barely moves, and it's just informational)
def get_weather(lat, lon):
    """Fetches current weather from Open-Meteo (Free, No Key)."""
    try:
        # Ensure floats
        lat = float(lat)
        lon = float(lon)

        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,weather_code&temperature_unit=fahrenheit&timezone=auto"
        headers = {'User-Agent': '5GSecurityJobBoard/1.0'}
        r = requests.get(url, headers=headers, timeout=3)
        
        if r.status_code != 200:
            return None
            
        data = r.json()
        
        if 'error' in data:
            return None

        current = data.get('current', {})
        temp = current.get('temperature_2m')
        code = current.get('weather_code')
        
        if temp is None:
            return None
            
        # Ensure temp is a valid number
        try:
            float(temp)
        except (ValueError, TypeError):
            return None
        
        # Simple WMO code map
        condition = "Unknown"
        if code is not None:
            try:
                code = int(code)
                if code == 0: condition = "☀️ Clear"
                elif code in [1, 2, 3]: condition = "⛅ Partly Cloudy"
                elif code in [45, 48]: condition = "🌫️ Foggy"
                elif code in [51, 53, 55]: condition = "🌧️ Drizzle"
                elif code in [61, 63, 65]: condition = "🌧️ Rain"
                elif code in [71, 73, 75]: condition = "❄️ Snow"
                elif code in [95, 96, 99]: condition = "⛈️ Thunderstorm"
            except (ValueError, TypeError):
                pass
            
        return f"{condition} {temp}°F"
    except Exception as e:
        return None
