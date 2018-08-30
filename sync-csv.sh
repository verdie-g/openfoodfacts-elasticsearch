#!/bin/sh

csv='en.openfoodfacts.org.products.csv'
csv_url="https://world.openfoodfacts.org/data/$csv"
mapping='fields-mapping.json'

echo "Downloading OpenFoodFacts CSV data export"
wget -q --show-progress $csv_url

echo "Insert products to ElasticSearch"
go run elasticsearch-csv.go product.go error.go $csv $mapping
