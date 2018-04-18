#!/bin/sh

mongo_dump='https://world.openfoodfacts.org/data/openfoodfacts-mongodbdump.tar.gz'
mongo_dump_checksum='https://world.openfoodfacts.org/data/md5sum'

dump_archive='openfoodfacts-mongodbdump.tar.gz'
dump_folder='dump'
database_default='off'
collection='products'

database='offDB'

mongolastic="mongolastic.jar"
mongolastic_repo="ozlerhakan/mongolastic"
mongolastic_cfg="mongolastic.yml"

es='http://localhost:9200'
index='off'
mapping='fields-mapping.json'

set -e

[[ $EUID -ne 0 ]] && echo "This script must be run as root." && exit 1

if [ -f "$dump_archive" ]; then
  mv "$dump_archive" "$dump_archive".old
fi

echo "Downloading OpenFoodFacts mongo dump"
wget -q --show-progress "$mongo_dump"

echo "Computing md5sum"
local_md5=$(md5sum "$dump_archive")

echo "Downloading checksum"
online_md5=$(wget -q -O- "$mongo_dump_checksum")
if [ "$local_md5" != "$online_md5" ]; then
  echo 'Downloaded file is corrupted'
  exit 1
fi

echo "Decompressing dump_archive"
rm -rf "$dump_folder"
tar xf "$dump_archive" --verbose

echo "Rename database to $database"
sed -i "s/$database_default/$database/g" "$dump_folder/$database_default/$collection.metadata.json"
mv "$dump_folder/$database_default" "$dump_folder/$database"

echo "Restoring mongoDB database"
systemctl start mongodb
mongorestore "$dump_folder"

if [ ! -f "$mongolastic" ]; then
  echo "Downloading mongolastic"
  mongolastic_mongo_dump=$(cmongo_dump -s "https://api.github.com/repos/$mongolastic_repo/releases/latest" | grep browser_download_mongo_dump | cut -d '"' -f 4)
  wget -q --show-progress "$mongolastic_mongo_dump"
fi

echo "Migrating from MongoDB to ElasticSearch"
cmongo_dump -X DELETE "$es/$index"
cmongo_dump -X PUT -H 'Content-Type: application/json' "$es/$index" --data "@$mapping"
java -jar "$mongolastic" -f "$mongolastic_cfg"

echo "Purging MongoDB $database.$collection"
mongo $database --eval "'db.$collection.drop()'" --quiet

systemctl stop mongodb

echo 'Done.'
