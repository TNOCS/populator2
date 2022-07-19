The purpose of this project is to update the [populator](https://github.com/TNOCS/populator) service with more recent data.

The bag enrichment guide in the original populator repo unfortunately has some problems with more recent data.

The first problem is the fact that the populator service is based on BAG V1 data. BAG V1 has been deprecated a while after the introduction of BAG V2. The software used to process the BAG data [NLExtract](https://github.com/nlextract/NLExtract) has been updated to handle BAG V2 but development has slowed significantly.

Secondly, the origin of some essential data used in the generation process seems to be undocumented. (buurten_XXX.csv) This csv needs to be reverse engineered.