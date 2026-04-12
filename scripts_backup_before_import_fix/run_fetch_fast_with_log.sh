#!/bin/bash
LOGFILE="/home/abubakar/intelliprrsv2/logs/fetch_fast.log"
cd /home/abubakar/intelliprrsv2
source venv/bin/activate
python3 /home/abubakar/intelliprrsv2/scripts/fetch_ncbi_fast.py >> $LOGFILE 2>&1
deactivate

