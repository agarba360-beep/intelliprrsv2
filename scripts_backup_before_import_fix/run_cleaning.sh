#!/bin/bash
LOGFILE="/home/abubakar/intelliprrsv2/logs/cleaning.log"
cd /home/abubakar/intelliprrsv2
source venv/bin/activate
python3 scripts/clean_sequences.py >> $LOGFILE 2>&1
deactivate

