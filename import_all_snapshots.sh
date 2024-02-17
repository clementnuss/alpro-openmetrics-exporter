#!/bin/env bash

SNAPSHOTS=$(restic snapshots | tail -n +3 | head -n -3 | awk '{print $1}')

for snapshot in $SNAPSHOTS; do
    echo restoring snapshot id: "$snapshot"
    restic restore --target ./restore "$snapshot"
    find ./restore -type f -exec mv {} ./ \;
    bash convert.sh
    ls -lsah alpro.db
    python3 alpro-to-openmetrics.py --history
done