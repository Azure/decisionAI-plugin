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
    version="0.0.75",
    description="Time series analysis plugin",
    long_description="An plugin package for time series analysis, 3rd parties could implement their own train/inference.",
    long_description_content_type="text/markdown",
    license="MIT Licence",
    url="https://github.com/Azure/decisionAI-plugin",
    author="Chunlei Wang",
    author_email="chuwan@microsoft.com",
    #packages=['decisionai_plugin'],
	packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
	install_requires=[
		'pyyaml==6.0',
		'Flask==3.0.3',
		'flask_restful==0.3.9',
		'requests>=2.31.0',
		'python-dateutil==2.8.2',
		'azure-storage-blob==12.22.0',
		'azure-data-tables==12.5.0',
		'Werkzeug==3.0.3',
		'gunicorn==22.0.0',
        'gevent==23.9.1',
        'greenlet==2.0.1',
	    'apscheduler==3.8.0',
        'numpy==1.22.3',
        'pandas==1.2.2',
        'azure-identity==1.17.1',
        'kafka-python==2.0.2',
        'ruamel.yaml==0.16.10',
        'confluent_kafka==2.0.2',
    ],
	include_package_data=True
)
