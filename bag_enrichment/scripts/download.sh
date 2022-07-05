#!/usr/bin/env bash

BAGFILE=lvbag-extract-nl.zip
BAGDLLINK=https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip
WBKAARTFILE=wijkbuurtkaart_2022_v0.zip
WBKAARTDLLINK=https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/wijkbuurtkaart_2022_v0.zip

# Downloading bag file to processing folder
if [ -f /data/processing/$BAGFILE ]; then
   echo "File /data/processing/$BAGFILE exists. Skipping download."
else
   echo "File /data/processing/$BAGFILE does not exist. Starting download."
   wget $BAGDLLINK -P /data/processing;
fi

# Downloading wijkbuurtkaart to processing folder
if [ -f /data/processing/$WBKAARTFILE ]; then
   echo "File /data/processing/$WBKAARTFILE exists. Skipping download."
else
   echo "File /data/processing/$WBKAARTFILE does not exist. Starting download."
   wget $WBKAARTDLLINK -P /data/processing;
fi