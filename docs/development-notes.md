The default bridge network in docker does not allow containers to connect each other via container names used as dns hostnames. Therefore all containers connect to the user defined 'populator' network.


NLextract with BAG V2
Bag V1 has been replace by BAG V2. The program used to process BAG data is called NLExtract. While it has been updated to handle BAG V2, development has slowed significantly. Only part of the documentation has been updated. This is the reason the setup script uses the 'test' schema. Test is used in the documentation. Only at a later point in the script the schema is renamed to bagv2.