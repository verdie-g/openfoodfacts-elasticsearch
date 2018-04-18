# Open Food Facts data to ElasticSearch

## sync-csv.sh

Download and parse CSV data exports and bulk insert into ElasticSearch.

See elasticsearch-csv.go and product.go to edit index name, type name, ...

Use this script, not the following one.

## sync-mongo-dump.sh

Download MongoDB dump, restore it and use [mongolastic](https://github.com/ozlerhakan/mongolastic)
to migrate to ElasticSearch.

## Result

First packaging geo points visualized on Kibana.

![30698474_10204251322279501_8626103993363333120_n](https://user-images.githubusercontent.com/9092290/38835795-6bb9592a-41cc-11e8-818f-3adf4781ecbc.png)
