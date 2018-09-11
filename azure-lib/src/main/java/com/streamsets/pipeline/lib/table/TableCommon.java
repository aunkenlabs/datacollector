package com.streamsets.pipeline.lib.table;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;

public class TableCommon {
  private static final Logger LOG = LoggerFactory.getLogger(TableCommon.class);
  private CloudTableClient tableClient;


  public TableCommon(String storageAccountConnectionString) throws URISyntaxException, InvalidKeyException {
    CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageAccountConnectionString);
    tableClient = storageAccount.createCloudTableClient();
  }

  public void insertAndCreate(String tableName, String partitionKey, String rowKey, String content) throws URISyntaxException, StorageException {
    try {
      insert(tableName, partitionKey, rowKey, content);
    } catch (TableServiceException e) {
      if (e.getErrorCode().equals("TableNotFound")) {
        createTableIfNotExists(tableName);
        insert(tableName, partitionKey, rowKey, content);
      } else
        throw e;
    }
  }

  public void insert(String tableName, String partitionKey, String rowKey, String content) throws URISyntaxException, StorageException {
    LOG.info("Writing on table \"{}\" partitionKey: {} rowKey: {}", tableName, partitionKey, rowKey);

    CloudTable cloudTable = tableClient.getTableReference(tableName);
    TableOperation insertOrReplace = TableOperation.insertOrReplace(
      new TableEntity(partitionKey, rowKey, content.getBytes(StandardCharsets.UTF_8))
    );
    cloudTable.execute(insertOrReplace);
  }

  public void createTableIfNotExists(String tableName) throws URISyntaxException, StorageException {
    LOG.info("Creating table \"{}\"", tableName);

    CloudTable cloudTable = tableClient.getTableReference(tableName);
    cloudTable.createIfNotExists();
  }

}
