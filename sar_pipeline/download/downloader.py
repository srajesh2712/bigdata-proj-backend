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
    print(access_token)
else:
    raise Exception(f"Authentication failed: {response.text}")

# Step 2: Use the access token to query and download products
api_url = "https://sh.dataspace.copernicus.eu/api/v1/process"  # Update this to the correct query API if necessary

# Pass the token to SentinelAPI
headers = {"Authorization": f"Bearer {access_token}"}
search_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

response = requests.get(search_url, headers=headers)
print(response.json())

def download_sentinel1_data(product_id, output_dir):
    """
    Simulate download (you’ll replace this with your logic).
    """
    print(f"Downloading product {product_id} to {output_dir}...")
    # Simulate download
    return f"{output_dir}/{product_id}.zip"
