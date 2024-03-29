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

# Create populator network
docker network create populator

# creating processing volume
docker volume create populator-processing

# Exporting variables for use in scripts
rm ./scripts/set-environment.sh 2> /dev/null
echo "BAGFILE=$BAGFILE" >> ./scripts/set-environment.sh
echo "BAGDLLINK=$BAGDLLINK" >> ./scripts/set-environment.sh
echo "WBKAARTFILE=$WBKAARTFILE" >> ./scripts/set-environment.sh
echo "WBKAARTDLLINK=$WBKAARTDLLINK" >> ./scripts/set-environment.sh

# creating and preparing debian container
docker run -d -it \
    --name populator-debian \
    --network populator \
    -v populator-processing:/data \
    debian:stable
docker cp $(pwd)/scripts populator-debian:/data
docker exec -ti populator-debian bash /data/scripts/install-required-programs.sh

# debian container downloads source files to volume and extract
docker exec -ti populator-debian bash /data/scripts/download.sh
docker exec -ti populator-debian bash /data/scripts/extract.sh

# creating persistence volume for postgis
docker volume create populator-pgdata

# Create postgis container with bagv2 db and wait until it has booted
echo "creating postgis db"
docker run -d \
    --name populator-postgis \
    --network populator \
    -p 5432:5432 \
    -v populator-pgdata:/var/lib/postgresql/data \
    -v populator-processing:/data \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=bagv2 \
    postgis/postgis
echo "Waiting for db to boot..."
sleep 10;
while [ "$(docker exec -ti populator-postgis psql -U postgres -c "\l" | grep -c "bagv2            | postgres | UTF8     | en_US.utf8 | en_US.utf8 |")" != 1 ]; 
do 
    echo "Waiting for db to boot...";
    sleep 2; 
done

# Create db schema needed for nlextract (source: https://github.com/nlextract/NLExtract/tree/master/bagv2/etl)
docker exec -it populator-postgis psql -U postgres  -c "\connect bagv2;" -c "CREATE SCHEMA test;";

# Check if test schema is created
if [ "$(docker exec -ti populator-postgis psql -U postgres -c "\connect bagv2;" -c "\dn" | grep -c test)" == 1 ];
then
    echo "test schema is found.  Continuing.";
else
     exit_with_error "Something went wrong creating the 'test' schema. Aborting.";
fi

# Create nlextract container that processes bag into the postgis db. Processing is single threaded... ETA 3h on ryzen 3600
sleep 1;
docker run --rm \
    --name populator-nlextract \
    --network populator \
    -v populator-processing:/data \
    nlextract/nlextract:latest \
    bagv2/etl/etl.sh -a bag_input_file=/data/processing/$BAGFILE -a pg_host=populator-postgis;
docker exec -it populator-postgis psql -U postgres -c "\connect bagv2;" -c "DROP SCHEMA IF EXISTS bag CASCADE;";
docker exec -it populator-postgis psql -U postgres -c "\connect bagv2;" -c "ALTER SCHEMA test RENAME TO bag;";

# Creating cbs schema and processing SHP files from wijkbuurtkaart into the postgis db
docker exec -ti populator-postgis psql -U postgres -c "\connect bagv2;" -c "CREATE SCHEMA cbs;"
docker exec -ti populator-postgis bash -c "apt update && apt install postgis -y";
docker exec -ti populator-postgis bash -c "shp2pgsql -I -d -s 0:28992 /data/processing/SHP/CBS_gemeenten2022.shp cbs.gemeenten | psql -U postgres -d bagv2";
docker exec -ti populator-postgis bash -c "shp2pgsql -I -d -s 0:28992 /data/processing/SHP/CBS_buurten2022.shp cbs.buurten | psql -U postgres -d bagv2";
docker exec -ti populator-postgis bash -c "shp2pgsql -I -d -s 0:28992 /data/processing/SHP/CBS_wijken2022.shp cbs.wijken | psql -U postgres -d bagv2";

# Stopping and removing debian container
docker stop populator-debian;
docker rm populator-debian;

# Deleting populator-processing container
docker volume rm populator-processing;

echo "The BAG setup script has concluded.";


# docker exec -ti populator-postgis psql -U postgres
