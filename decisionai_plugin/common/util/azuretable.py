from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient, UpdateMode
from azure.identity import DefaultAzureCredential

from .constant import AZURE_STORAGE_ACCOUNT_USE_MI


class AzureTable():
    def __init__(self, account_name, account_key=None, account_domain="core.windows.net"):
        cred = DefaultAzureCredential()
        if not AZURE_STORAGE_ACCOUNT_USE_MI:
            cred = AzureNamedKeyCredential(account_name, account_key)
        self.table_service_client = TableServiceClient(endpoint="https://{}.table.{}".format(account_name, account_domain), credential=cred)

    def create_table_if_not_exists(self, table_name):
        return self.table_service_client.create_table_if_not_exists(table_name)

    def exists_table(self, table_name):
        list_tables = self.table_service_client.list_tables()
        return table_name in list_tables

    def insert_or_replace_entity(self, table_name, partition_key, row_key, **kwargs):
        try:
            entity = self.table_service_client.get_table_client(table_name).get_entity(partition_key, row_key)
        except Exception:    
            # Insert a new entity
            entity = {'PartitionKey': partition_key, 'RowKey': row_key}
        
        for (k,v) in kwargs.items():
            entity[k] = v

        return self.table_service_client.get_table_client(table_name).upsert_entity(entity, UpdateMode.REPLACE)

    def insert_or_replace_entity2(self, table_name, entity):
        return self.table_service_client.get_table_client(table_name).upsert_entity(entity, UpdateMode.REPLACE)

    def insert_entity(self, table_name, entity):
        return self.table_service_client.get_table_client(table_name).create_entity(entity)

    def update_entity(self, table_name, entity):
        return self.table_service_client.get_table_client(table_name).update_entity(entity)

    def get_entity(self, table_name, partition_key, row_key):
        return self.table_service_client.get_table_client(table_name).get_entity(partition_key, row_key)

    def delete_entity(self, table_name, partition_key, row_key):
        return self.table_service_client.get_table_client(table_name).delete_entity(partition_key, row_key)

    def delete_table(self, table_name):
        return self.table_service_client.delete_table(table_name)
    
    def get_entities(self, table_name, partition_key):
        filter = "PartitionKey eq '{0}'".format(partition_key)
        return self.table_service_client.get_table_client(table_name).query_entities(filter)

