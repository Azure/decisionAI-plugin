import time
import logging
import uuid
from os import environ

from .azureblob import AzureBlob
from .azuretable import AzureTable
from .constant import AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_TABLE_KEY, AZURE_STORAGE_ACCOUNT_DOMAIN

from telemetry import log

thumbprint = str(uuid.uuid1())

def init_monitor(config): 
    azure_table = AzureTable(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_TABLE_KEY, AZURE_STORAGE_ACCOUNT_DOMAIN)
    if not azure_table.exists_table(config.az_tsana_moniter_table):
        azure_table.create_table(config.az_tsana_moniter_table)
    tk = time.time()
    azure_table.insert_or_replace_entity(config.az_tsana_moniter_table, config.tsana_app_name, 
                        thumbprint, 
                        ping = tk)

def run_monitor(config): 
    azure_table = AzureTable(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_TABLE_KEY, AZURE_STORAGE_ACCOUNT_DOMAIN)
    if not azure_table.exists_table(config.az_tsana_moniter_table):
        return

    tk = time.time()
    azure_table.insert_or_replace_entity(config.az_tsana_moniter_table, config.tsana_app_name, 
                        thumbprint, 
                        ping = tk)    

def stop_monitor(config):
    log.info('Monitor exit! ')

    try: 
        azure_table = AzureTable(AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_TABLE_KEY, AZURE_STORAGE_ACCOUNT_DOMAIN)
        if not azure_table.exists_table(config.az_tsana_moniter_table):
            return
        azure_table.delete_entity(config.az_tsana_moniter_table, config.tsana_app_name, 
                            thumbprint)       
    except:
        pass
