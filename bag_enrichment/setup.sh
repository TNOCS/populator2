#!/usr/bin/env bash

BAGFILE=lvbag-extract-nl.zip
BAGDLLINK=https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip
WBKAARTFILE=wijkbuurtkaart_2022_v0.zip
WBKAARTDLLINK=https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/wijkbuurtkaart_2022_v0.zip


exit_with_error()
{
    echo "Fatal error: $1" >&2
    # TODO: send email/notification?
    exit 1
}

if ! [ -x "$(command -v docker)" ]; then
  exit_with_error "I require docker but it's not installed.  Aborting."
fi
if ! [ -x "$(command -v 7z)" ]; then
  exit_with_error "I require 7z but it's not installed.  Aborting."
fi
if ! [ -x "$(command -v wget)" ]; then
  exit_with_error "I require wget but it's not installed.  Aborting."
fi

# creating processing folder
mkdir processing;

# Downloading bag file to processing folder
if [ -f ./processing/$BAGFILE ]; then
   echo "File ./processing/$BAGFILE exists. Skipping download."
else
   echo "File ./processing/$BAGFILE does not exist. Starting download."
   wget $BAGDLLINK -P ./processing;
fi

# Downloading wijkbuurtkaart to processing folder
if [ -f ./processing/$WBKAARTFILE ]; then
   echo "File ./processing/$WBKAARTFILE exists. Skipping download."
else
   echo "File ./processing/$WBKAARTFILE does not exist. Starting download."
   wget $WBKAARTDLLINK -P ./processing;
fi

# extracting wijkbuurtkaart to processing folder
7z x ./processing/$WBKAARTFILE -aoa -o./processing;

# Create folders for postgis for persistence
mkdir -p persistence/postgresql/data;

# Create postgis container with bagv2 db and wait until it has booted
echo "creating postgis db"
docker run --name populator-postgis -p 5432:5432 -v $(pwd)/persistence/postgresql/data:/var/lib/postgresql/data -v $(pwd)/processing/:/processing -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=bagv2 -d postgis/postgis
echo "Waiting for db to boot..."
sleep 10;
while [ "$(docker exec -ti populator-postgis psql -U postgres -c "\l" | grep -c "bagv2            | postgres | UTF8     | en_US.utf8 | en_US.utf8 |")" != 1 ]; 
do 
    echo "Waiting for db to boot...";
    sleep 1; 
done

# Create db schema needed for nlextract (source: https://github.com/nlextract/NLExtract/tree/master/bagv2/etl)
sleep 1;
docker exec -it populator-postgis psql -U postgres  -c "\connect bagv2;" -c "CREATE SCHEMA test;";

# Check if test schema is created
if [ "$(docker exec -ti populator-postgis psql -U postgres -c "\connect bagv2;" -c "\dn" | grep -c test)" == 1 ];
then
    echo "test schema is found.  Continuing.";
else
     exit_with_error "test schema is not found";
fi

# Create nlextract container that processes bag into the postgis db
sleep 1;
##  TODO check if postgis accepts connections on pg_host
docker run --name nlextract --rm -v $(pwd)/processing:/processing nlextract/nlextract:latest bagv2/etl/etl.sh -a bag_input_file=/processing/$FILE -a pg_host=172.17.0.1;
docker exec -it populator-postgis psql -U postgres -c "\connect bagv2;" -c "DROP SCHEMA IF EXISTS bag CASCADE;";
docker exec -it populator-postgis psql -U postgres -c "\connect bagv2;" -c "ALTER SCHEMA test RENAME TO bag;";

# Creating cbs schema and processing SHP files from wijkbuurtkaart into the postgis db
docker exec -ti populator-postgis psql -U postgres -c "\connect bagv2;" -c "CREATE SCHEMA cbs;"
docker exec -ti populator-postgis bash -c "apt update && apt install postgis -y";
docker exec -ti populator-postgis bash -c "shp2pgsql -I -d -s 0:28992 /processing/SHP/CBS_gemeenten2022.shp cbs.gemeenten | psql -U postgres -d bagv2";
docker exec -ti populator-postgis bash -c "shp2pgsql -I -d -s 0:28992 /processing/SHP/CBS_buurten2022.shp cbs.buurten | psql -U postgres -d bagv2";
docker exec -ti populator-postgis bash -c "shp2pgsql -I -d -s 0:28992 /processing/SHP/CBS_wijken2022.shp cbs.wijken | psql -U postgres -d bagv2";


echo "The BAG setup script has concluded.";


# docker exec -ti populator-postgis psql -U postgres
