import requests
from sentinelsat import SentinelAPI, geojson_to_wkt, read_geojson

# Step 1: Authenticate with the Copernicus Data Space Ecosystem
auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
payload = {
    "username": "srajesh2712@gmail.com",
    "password": "Rajesh@27121984",
    "grant_type": "password",
    "client_id": "cdse-public"
}
response = requests.post(auth_url, data=payload)

if response.status_code == 200:
    access_token = response.json().get("access_token")
    print("Authentication successful!")
else:
    raise Exception(f"Authentication failed: {response.text}")

# Step 2: Use the access token to query and download products
api_url = "https://apihub.dataspace.copernicus.eu/apihub"  # Update this to the correct query API if necessary

# Pass the token to SentinelAPI
headers = {"Authorization": f"Bearer {access_token}"}
api = SentinelAPI("srajesh2712@gmail.com", "Rajesh@27121984", api_url)

# Define your area of interest
footprint = geojson_to_wkt(read_geojson('your_aoi.geojson'))

# Query GRD products
products = api.query(
    footprint,
    date=('20240101', '20240601'),
    platformname='Sentinel-1',
    producttype='GRD',
    sensoroperationalmode='IW',
    polarisationmode='VV VH'
)

# Print number of products found
print(f"Number of products found: {len(products)}")

# Download all matching products
api.download_all(products)
