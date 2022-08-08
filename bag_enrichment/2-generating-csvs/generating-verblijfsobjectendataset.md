In the population generation script you'll need a verblijfsobjecten dataset.
Save the output of the following query as verblijfsobjecten_nederland.csv

```sql
select bu.bu_code, vbo.identificatie, vbo.oppervlakteverblijfsobject, vbogd.gebruiksdoelverblijfsobject
from bag.verblijfsobjectactueelbestaand vbo
left join cbs.buurten bu
on st_contains(bu.geom, vbo.geopunt)
left join bag.verblijfsobjectgebruiksdoelactueelbestaand vbogd
on vbogd.identificatie = vbo.identificatie
```

This query can take a long time.
If you just want to test this process out and just generate a much smaller population for 1 municipality (Dordrecht), save the output of the following query as verblijfsobjecten_doordrecht.csv

```sql
select bu.bu_code, vbo.identificatie, vbo.oppervlakteverblijfsobject, vbogd.gebruiksdoelverblijfsobject
from bag.verblijfsobjectactueelbestaand vbo
left join cbs.buurten bu
on st_contains(bu.geom, vbo.geopunt)
left join bag.verblijfsobjectgebruiksdoelactueelbestaand vbogd
on vbogd.identificatie = vbo.identificatie
where bu.gm_code = 'GM0505'
```
