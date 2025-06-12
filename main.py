import os

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from dotenv import load_dotenv
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt


load_dotenv()

def connect_to_mongo():
    MONGOLAB_URI = os.getenv('MONGOLAB_URI')
    # creating mongo client
    client = MongoClient(MONGOLAB_URI, server_api = ServerApi('1'))

    # sending a ping to test connection

    try:
        client.admin.command('ping')
        print(' pinged to Mongodb . successfully connected ')
    except Exception as e:
        print(e)



# Connect to Copernicus API (replace with your username and password)
#curl -s -X POST https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token -H "Content-Type: application/x-www-form-urlencoded" -d "username=srajesh2712@gmail.com" -d "password=Rajesh@27121984" -d "grant_type=password" -d "client_id=cdse-public"
api = SentinelAPI('your_username', 'your_password', 'https://scihub.copernicus.eu/dhus')

# Define your area of interest (GeoJSON or WKT)
footprint = geojson_to_wkt(read_geojson('your_aoi.geojson'))

# Query GRD products for a date range and cloud cover
products = api.query(footprint,
                     date=('20240101', '20240601'),
                     platformname='Sentinel-1',
                     producttype='GRD',
                     sensoroperationalmode='IW',
                     polarisationmode='VV VH')

# Download all matching products
api.download_all(products)
