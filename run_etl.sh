#!/bin/bash

# Virtual Environment Path (ubah sesuai jalur venv Anda di WSL)
VENV_PATH="/mnt/e/Goals/pacmann/venv/bin/activate"

# Aktivasi virtual environment
source "$VENV_PATH"

# Set Path ke Python Script
PYTHON_SCRIPT="/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/etl_de_project_pacmann.py"

# Jalankan Python script dan simpan output log
python "$PYTHON_SCRIPT" >> /mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/logfile.log 2>&1
