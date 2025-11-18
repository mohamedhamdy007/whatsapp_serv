from azure.identity import DefaultAzureCredential
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import logging
import os
# === Replace with your actual credentials ===

class Azure_cosmos():
    def __init__(self,url,database,container):
        self.url=url
        self.database=database
        self.container=container
    def connect(self):
        credential = DefaultAzureCredential()
        try:    
            client = CosmosClient(self.url,credential)
        except Exception as e:
            logging.error(f"Error initializing CosmosClient: {e}")
            client = None
            return  {"status":False,"database":None,"container":None,"comment":"Error initializing CosmosClient" }

        if client:
            database = client.get_database_client(self.database)
            container = database.get_container_client(self.container)
            self.container =container
            self.database=database
            return {"status":True,"database":database,"container":database,"client":client}
        else:
            logging.error("Error initializing CosmosClient")
            container = None
            return  {"status":False,"database":None,"container":None,"comment":"Error initializing CosmosClient with containers" }

    def insert_item(self, data):
        """
        Insert a document into the Cosmos DB container.
        Args:
            data (dict): The JSON document to insert.
            container: The Cosmos container client object.
        """
        
        if not self.container:
            logging.error("Invalid container reference passed to insert()")
            return {"status": False, "comment": "Invalid container reference"}
        try:
            response = self.container.create_item(body=data)
            logging.info(f"Document inserted successfully. ID: {response.get('id', 'N/A')}")
            return {"status": True, "response": response}
        except exceptions.CosmosHttpResponseError as http_err:
            logging.error(f"Cosmos HTTP error during insert: {http_err}")
            return {"status": False, "comment": str(http_err)}
        except Exception as e:
            logging.error(f"Unexpected error during insert: {e}")
            return {"status": False, "comment": str(e)}

        
        