#!/bin/bash
set -e

rm -rvf /tmp/streamsets-datacollector-azure-lib /tmp/test-data/ target

mkdir -p /tmp/test-data/
for FILE in $(ls -1 test-data/*.gz); do
    DEST=$(echo "${FILE}" | tr -d '.gz')
    gzip -dc ${FILE} > /tmp/${DEST}.json
done

mvn clean install -DskipTests
tar zxvf target/streamsets-datacollector-azure-lib-3.4.0-SNAPSHOT.tar.gz -C /tmp

docker run --rm -it \
    --name sdc \
    -v /tmp/streamsets-datacollector-azure-lib/lib:/opt/streamsets-datacollector-3.2.0.0/streamsets-libs/streamsets-datacollector-azure-lib/lib \
    -v /tmp/data:/data \
    -v /tmp/test-data:/tmp/test-data \
    -p "18630:18630" \
    streamsets/datacollector:3.2.0.0