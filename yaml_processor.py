import yaml
from dotenv import load_dotenv
import os
from azure.cosmos import CosmosClient
from uuid import uuid1

load_dotenv()

DATABASE_NAME = os.getenv('COSMOS_DB_NAME')
COSMOS_ENDPOINT = os.getenv('COSMOS_ENDPOINT')
COSMOS_KEY = os.getenv('COSMOS_KEY')

cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)

database = cosmos_client.get_database_client(DATABASE_NAME)

COSMOS_CONTAINERS = ['configs']

container_clients = dict()

for container_name in COSMOS_CONTAINERS:
    container_clients[container_name] = database.get_container_client(container_name)

def upload_to_cosmos(container_name, item): 
    container = container_clients[container_name]
    
    saved_item = container.upsert_item(item)
    return saved_item

def get_from_cosmos(container_name = '', query = ''):
    return list(container_clients[container_name].query_items(
        query=query,
        enable_cross_partition_query=True
    ))
