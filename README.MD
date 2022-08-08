# Populator2

The purpose of this project is to update the [populator](https://github.com/TNOCS/populator) service with more recent data.

The populator service relies on a database filled with data generated from multiple sources.
The data generation is called 'bag enrichment' in the populator repo. The data generation process will be rewritten to be easier to understand and replicate.

## Requirements

The scripts in the repo have been tested on the following systems:

- Ubuntu 20.04 on WSL2 on Windows 10
- Debian 11.4 QEMU virtual machine

The following software needs to be installed:
[Docker](https://docs.docker.com/get-docker/)
[Python](https://www.python.org/downloads/)

## Challenges

### The bag enrichment guide links to data sources that are no longer available.

The bag enrichment guide has some problems dealing with more recent data. It uses BAG V1 data. BAG V1 has been replaced by BAG V2.

### The bag enrichment guide links to software that is outdated / no longer available.

The bag enrichment process will be rewritten to use docker to standardize the environment.
Also, the software used to process the BAG data in the original populator guide ([NLExtract](https://github.com/nlextract/NLExtract)) has been updated to handle BAG V2 but the documentation has not been fully updated and development has slowed significantly.

## Difficult to reproduce some data

The origin of some essential data used in the generation process seems to be undocumented. (e.g. buurten_XXX.csv)
