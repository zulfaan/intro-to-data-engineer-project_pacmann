#!/bin/bash

# Virtual Environment Path (ubah sesuai jalur venv Anda di WSL)
VENV_PATH="/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/venv/bin/activate"

# Aktivasi virtual environment
source /mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/venv/bin/activate

# Cek apakah python sudah benar dari venv
which python

# Set Path ke Python Script
PYTHON_SCRIPT="/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/etl_de_project_pacmann.py"

# Jalankan Python script dan simpan output log
/mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/venv/bin/python "$PYTHON_SCRIPT" >> /mnt/e/Goals/pacmann/Learn/Python/case-study-ETL-workflow/intro-to-data-engineer-project_pacmann/logfile.log 2>&1
