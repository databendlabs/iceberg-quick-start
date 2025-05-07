#!/bin/bash

cd /home/iceberg

python -m venv .venv
source .venv/bin/activate
pip install duckdb pyspark

python load_tpch.py

