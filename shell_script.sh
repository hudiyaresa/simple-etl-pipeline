#!/bin/bash

# Virtual Environment Path
VENV_PATH="/venv/bin/activate"

# Activate venv
source "$VENV_PATH"

# set python script
PYTHON_SCRIPT="etl_luigi.py"

# run python script and logging
python "$PYTHON_SCRIPT" >> log/logfile.log 2>&1
