from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import generate_container_sas, generate_blob_sas, BlobSasPermissions 

from datetime import datetime
from datetime import timedelta

from telemetry import log

class AzureBlob():
    def __init__(self, account_name, account_key):
        connect_str = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(account_name, account_key)
        # Create the BlobServiceClient object which will be used to create a container client
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)    

    def create_container(self, container_name):
        # Create the container
        try:
            self.blob_service_client.create_container(container_name)
        except ResourceExistsError:
            log.info("Container %s already exists!" % container_name)

    def upload_blob(self, container_name, blob_name, data, replace = True):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        log.info("\nUploading to Azure Storage as blob:\n\t" + blob_name)

        # Upload the created file
        blob_client.upload_blob(data, overwrite=replace)
        log.info("Blob %s in container %s has been uploaded/updated!" % (blob_name, container_name))
            
    def list_blob(self, container_name):
        log.info("\nListing blobs...")

        # List the blobs in the container
        blob_list = self.blob_service_client.get_container_client(container_name).list_blobs()
        blobs = []
        for blob in blob_list:
            blobs.append(blob.name)

        return blobs   

    def delete_blob(self, container_name, blob_name):
        log.info("\nDelete blob...")

        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()

    def download_blob(self, container_name, blob_name, download_file_path):
        log.info("\nDownloading blob to \n\t" + download_file_path)

        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        with open(download_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

    def delete_container(self, container_name):
        log.info("Deleting blob container...")
        self.blob_service_client.delete_container(container_name)
    
    @staticmethod
    def generate_blob_sas(account_name, account_key, container_name, blob_name):
        log.info("Generating blob sas...")
        blob_sas = generate_blob_sas(account_name=account_name, account_key=account_key, container_name=container_name, blob_name=blob_name,
        permission=BlobSasPermissions(read=True), expiry=datetime.utcnow() + timedelta(days=1))

        return 'https://' + account_name +'.blob.core.windows.net/' + container_name + '/' + blob_name + '?' + blob_sas