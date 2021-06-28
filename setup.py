#!/usr/bin/env python
#-*- coding:utf-8 -*-

#############################################
# Filename: setup.py
# Author: Chunlei Wang
# Mail: chuwan@microsoft.com
# Created Time:  2020-06-16 19:17:34
#############################################

from setuptools import setup, find_packages

with open("decisionai_plugin/README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="decisionai_plugin",
    version="0.0.46",
    keywords = ("pip", "timeseries", "plugin"),
    description = "Time series analysis plugin",
    long_description = "An plugin package for time series analysis, 3rd parties could implement their own train/inference.",
    long_description_content_type="text/markdown",
    license = "MIT Licence",
    url = "https://github.com/Azure/decisionAI-plugin",
    author = "Chunlei Wang",
    author_email = "chuwan@microsoft.com",
    #packages = ['decisionai_plugin'],
	packages = find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
	install_requires=[
		'pyyaml',
		'Flask==1.1.1',
		'flask_restful',
		'requests',
		'python-dateutil',
		'azure-storage-blob==12.1.0',
		'azure-cosmosdb-table==1.0.6',
		'Werkzeug==0.16.0',
		'gunicorn==19.9.0',
        'gevent==1.4.0',
        'greenlet==0.4.14',
        #'gevent==1.3.4',
        #'greenlet==0.4.13',
		'apscheduler',
        'numpy==1.17.3',
        'pandas==1.0.0',
        'azure-identity==1.4.1',
        'kafka-python==2.0.2',
        'ruamel.yaml==0.16.10',
    ],
	include_package_data=True
)