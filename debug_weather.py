import requests
import urllib.parse
import json

def get_lat_lon_from_address(address):
    print(f"Geocoding: {address}")
    try:
        def query_open_meteo(query):
            if not query or not query.strip(): return {}
            encoded_query = urllib.parse.quote(query.strip())
            url = f"https://geocoding-api.open-meteo.com/v1/search?name={encoded_query}&count=1&language=en&format=json"
            headers = {'User-Agent': '5GSecurityJobBoard/1.0'}
            print(f"Querying: {url}")
            try:
                response = requests.get(url, headers=headers, timeout=5)
                print(f"Response: {response.status_code}")
                return response.json()
            except Exception as e:
                print(f"Request failed: {e}")
                return {}

        data = query_open_meteo(address)
        print(f"Full address data: {json.dumps(data, indent=2)}")
        
        if 'results' in data and data['results']:
            result = data['results'][0]
            return result.get('latitude'), result.get('longitude')
        
        parts = [p.strip() for p in address.split(',')]
        if len(parts) >= 2:
            potential_city = parts[1] if len(parts) >= 3 else parts[0]
            print(f"Fallback city: {potential_city}")
            
            data = query_open_meteo(potential_city)
            print(f"City data: {json.dumps(data, indent=2)}")
            if 'results' in data and data['results']:
                result = data['results'][0]
                return result.get('latitude'), result.get('longitude')

        return None, None
            
    except Exception as e:
        print(f"Error: {e}")
        return None, None

def get_weather(lat, lon):
    print(f"Getting weather for {lat}, {lon}")
    try:
        lat = float(lat)
        lon = float(lon)
        
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,weather_code&temperature_unit=fahrenheit&timezone=auto"
        print(f"Weather URL: {url}")
        r = requests.get(url, timeout=2)
        print(f"Weather Response Code: {r.status_code}")
        data = r.json()
        print(f"Weather Data: {json.dumps(data, indent=2)}")
        
        if 'error' in data:
            print(f"API Error: {data['error']}")
            return None

        current = data.get('current', {})
        temp = current.get('temperature_2m')
        code = current.get('weather_code')
        
        condition = "Unknown"
        if code is not None:
            if code == 0: condition = "☀️ Clear"
            elif code in [1, 2, 3]: condition = "⛅ Partly Cloudy"
            elif code in [45, 48]: condition = "🌫️ Foggy"
            elif code in [51, 53, 55]: condition = "🌧️ Drizzle"
            elif code in [61, 63, 65]: condition = "🌧️ Rain"
            elif code in [71, 73, 75]: condition = "❄️ Snow"
            elif code in [95, 96, 99]: condition = "⛈️ Thunderstorm"
            
        return f"{condition} {temp}°F"
    except Exception as e:
        print(f"Weather Error: {e}")
        return None

addr = "3115 128th St, Lubbock, TX 79423, USA"
lat, lon = get_lat_lon_from_address(addr)
print(f"Lat: {lat}, Lon: {lon}")
if lat and lon:
    print(get_weather(lat, lon))
