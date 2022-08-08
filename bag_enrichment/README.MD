# Bag enrichment

This folder contains everything needed to work on an updated dataset for the populator service.

# 0 Development Helpers

The docker compose file in this folder will locally host the original populator database. \
This will be the functional but outdated database created for the original populator service. \
This allowes you to compare this database with the new one. \
Be aware that it will download a very large docker image. \
To run it, cd into the 0-development-helpers directory and run the up-old-database.sh script.

# 1 Creating database

The `create-new-database.sh` script will download the latest file and process them into a postgis database.
Please update the following variables in the script before running it to the latest available versions of the files.

```bash
BAGFILE=lvbag-extract-nl.zip
BAGDLLINK=https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip
WBKAARTFILE=wijkbuurtkaart_2022_v0.zip
WBKAARTDLLINK=https://www.cbs.nl/-/media/cbs/dossiers/nederland-regionaal/wijk-en-buurtstatistieken/wijkbuurtkaart_2022_v0.zip
```

# 2 Generating CSV's

In the final 'population' generation script, some data in the form of CSV will be needed. \
The 'verblijfsobjectendataset' is generator from the new database. Run the SQL query contained in the generating-verblijfsobjecten.md file.

I am unable to recreate the 'buurtendataset' as of yet.

# 3 Generating population

This is the final part of generating the data for the populator service. It was an 1500 line python script. I have edited the script to be a bit smaller, but there are still some undefined variables that need to be fixed. Also, this script needs the 'buurtendataset', which I have not been able to recreate.
