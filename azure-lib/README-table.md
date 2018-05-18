# Azure Table Stage

This stage provides a Destination to write records to [Azure Table](https://azure.microsoft.com/en-us/services/storage/tables/).

## Configuration





## Recommendations

1. Read the [Table document overview](https://docs.microsoft.com/en-us/azure/cosmos-db/table-storage-overview) and [Table Design Guide](https://docs.microsoft.com/en-us/azure/cosmos-db/table-storage-design-guide)


## Known Limitations

1. The entire Record Content is serialized as `UTF-8` strings and saved into a `payload` field on the table. We do not support custom fields at this release.




----

## Build & Test

```bash
mvn clean package

docker run --rm -it \
    --name sdc \
    -v $(pwd)/target:/opt/streamsets-datacollector-3.2.0.0/streamsets-libs/streamsets-datacollector-azure-lib/lib/ \
    -v $(pwd)/pipeline:/data \
    -p "18630:18630" \
    streamsets/datacollector:3.2.0.0
```