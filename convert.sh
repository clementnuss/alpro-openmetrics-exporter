#!/bin/bash

rm alpro.{db,sql}

python3 ./access_dump.py ./sample-db.apw >> alpro.sql
python3 ./access_dump.py ./sample-db.aph >> alpro.sql
sqlite3 alpro.db < alpro.sql