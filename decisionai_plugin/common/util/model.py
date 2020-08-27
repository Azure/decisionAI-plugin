import time
from time import gmtime, strftime
import traceback
from werkzeug.utils import secure_filename
import re
import os
from os import environ
from os.path import basename
import sys
import logging
import shutil
import zipfile
import json
import uuid

from .azureblob import AzureBlob
from .azuretable import AzureTable
from .timeutil import get_time_offset, str_to_dt, dt_to_str
from .constant import TIMESTAMP, VALUE
from .constant import STATUS_SUCCESS, STATUS_FAIL

# Copy the model file to local prd directory, zip and upload to AzureBlob
# The training output and prd directory is calculated by config with subscription/model_id
# Parameters:
#   config: a dict object which should include MODEL_TMP_DIR, TSANA_APP_NAME
#   subscription: the name of the user
#   model_id: UUID for the model
# Return:
#   result: STATE_SUCCESS / STATE_FAIL
#   message: description of the result
def upload_model(config, subscription, model_id, model_dir):
    try:
        zip_dir = os.path.join(config.model_temp_dir, subscription + '_' + model_id + '_' + str(time.time()))
        os.makedirs(zip_dir, exist_ok=True)

        zip_file = os.path.join(zip_dir, "model.zip")
        with zipfile.ZipFile(zip_file, "w") as zf:
            src_files = os.listdir(model_dir)
            for file_name in src_files:
                full_file = os.path.join(model_dir, file_name)
                if os.path.isfile(full_file):
                    zf.write(full_file, basename(full_file))
                else: 
                    full_subdir = os.path.join(model_dir, file_name)
                    for root, dirs, files in os.walk(full_subdir):
                        for fn in files:
                            absfn = os.path.join(root, fn)
                            print(absfn)
                            zfn = absfn[len(model_dir)+len(os.sep):]
                            zf.write(absfn, zfn)
            zf.close()

        container_name = config.tsana_app_name
        azure_blob = AzureBlob(environ.get('AZURE_STORAGE_ACCOUNT'), environ.get('AZURE_STORAGE_ACCOUNT_KEY'))
        azure_blob.create_container(container_name)

        with open(zip_file, "rb") as data:
            azure_blob.upload_blob(container_name, subscription + '_' + model_id, data)
        return STATUS_SUCCESS, ''
    except Exception as e:
        return STATUS_FAIL, str(e)
    finally:
        shutil.rmtree(zip_dir, ignore_errors=True)

# Check if local prd directory contains up-to-date model, otherwise, try to download from blob
# Parameters:
#   config: a dict object which should include MODEL_TMP_DIR, TSANA_APP_NAME
#   subscription: the name of the user
#   model_id: UUID for the model
# Return:
#   result: STATE_SUCCESS / STATE_FAIL
#   message: description of the result
def download_model(config, subscription, model_id, model_dir): 
    try:
        zip_dir = os.path.join(config.model_temp_dir, subscription + '_' + model_id + '_' + str(time.time()))
        os.makedirs(zip_dir, exist_ok=True)

        # download from blob
        container_name = config.tsana_app_name
        azure_blob = AzureBlob(environ.get('AZURE_STORAGE_ACCOUNT'), environ.get('AZURE_STORAGE_ACCOUNT_KEY'))
        azure_blob.create_container(container_name)
        model_name = subscription + '_' + model_id
        
        zip_file = os.path.join(zip_dir, "model.zip")
        azure_blob.download_blob(container_name, model_name, zip_file)
        with zipfile.ZipFile(zip_file) as zf:
            shutil.rmtree(model_dir, ignore_errors=True)
            os.makedirs(model_dir, exist_ok=True)
            zf.extractall(path=model_dir)
        return STATUS_SUCCESS, ''
    except Exception as e:
        return STATUS_FAIL, str(e)
    finally:
        shutil.rmtree(zip_dir, ignore_errors=True)
        