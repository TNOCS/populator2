#!/usr/bin/env bash
source /data/scripts/set-environment.sh

# Download bag file to processing folder if it does not exist
if [ -f /data/processing/$BAGFILE ]; then
   echo "File /data/processing/$BAGFILE exists. Skipping download."
else
   echo "File /data/processing/$BAGFILE does not exist. Starting download."
   wget $BAGDLLINK -P /data/processing;
fi

# Download wijkbuurtkaart to processing folder if it does not exist
if [ -f /data/processing/$WBKAARTFILE ]; then
   echo "File /data/processing/$WBKAARTFILE exists. Skipping download."
else
   echo "File /data/processing/$WBKAARTFILE does not exist. Starting download."
   wget $WBKAARTDLLINK -P /data/processing;
fi