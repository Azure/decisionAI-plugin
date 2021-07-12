from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from azure.identity import DefaultAzureCredential

class AzureTable():
    def __init__(self, account_name, account_key=None, account_domain="core.windows.net"):
        if account_key:
            self.table_service = TableService(account_name=account_name, account_key=account_key, endpoint_suffix=account_domain)
        else:
            scope = "https://storage.azure.com/.default"
            self.table_service = TableService(account_name=account_name, sas_token=DefaultAzureCredential().get_token(scope)[0], endpoint_suffix=account_domain)

    def create_table(self, table_name):
        return self.table_service.create_table(table_name)

    def exists_table(self, table_name):
        return self.table_service.exists(table_name)

    def insert_or_replace_entity(self, table_name, partition_key, row_key, **kwargs):
        try:
            entity = self.table_service.get_entity(table_name, partition_key, row_key)
        except Exception:    
            # Insert a new entity
            entity = {'PartitionKey': partition_key, 'RowKey': row_key}
        
        for (k,v) in kwargs.items():
            entity[k] = v

        return self.table_service.insert_or_replace_entity(table_name, entity)

    def insert_or_replace_entity2(self, table_name, entity):
        return self.table_service.insert_or_replace_entity(table_name, entity)

    def insert_entity(self, table_name, entity):
        return self.table_service.insert_entity(table_name, entity)

    def update_entity(self, table_name, entity):
        return self.table_service.update_entity(table_name, entity)

    def get_entity(self, table_name, partition_key, row_key):
        return self.table_service.get_entity(table_name, partition_key, row_key)

    def delete_entity(self, table_name, partition_key, row_key):
        self.table_service.delete_entity(table_name, partition_key, row_key)

    def delete_table(self, table_name):
        return self.table_service.delete_table(table_name)
    
    def get_entities(self, table_name, partition_key):
        filter = "PartitionKey eq '{0}'".format(partition_key)
        return self.table_service.query_entities(table_name, filter)

