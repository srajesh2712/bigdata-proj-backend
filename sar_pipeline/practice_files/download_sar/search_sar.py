import requests

# Manchester Bounding Box (Approx)
bbox = "-2.3,53.3,-2.1,53.5"

# Search for Sentinel-1 GRD in the first week of Jan 2025
url = f"https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel1/search.json?startDate=2025-01-01T00:00:00Z&completionDate=2025-01-10T23:59:59Z&box={bbox}&productType=GRD"

try:
    response = requests.get(url)
    data = response.json()
    print(f"--- Sentinel-1 Scenes Found: {len(data['features'])} ---")
    for feat in data['features']:
        print(f"ID: {feat['id']}")
        print(f"Date: {feat['properties']['startDate']}")
        print(f"Orbit: {feat['properties']['orbitDirection']}")
        print("-" * 30)
except Exception as e:
    print(f"Error searching Copernicus: {e}")
