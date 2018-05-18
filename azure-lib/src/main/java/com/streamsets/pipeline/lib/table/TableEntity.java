package com.streamsets.pipeline.lib.table;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class TableEntity extends TableServiceEntity {

  public TableEntity(String partitionKey, String rowKey, String payload) {
    this.partitionKey = partitionKey;
    this.rowKey = rowKey;
    this.payload = payload;
  }

  private String payload;

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }

}
