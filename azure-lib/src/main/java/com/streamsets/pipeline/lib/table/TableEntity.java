package com.streamsets.pipeline.lib.table;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class TableEntity extends TableServiceEntity {

  public TableEntity(String partitionKey, String rowKey, byte[] binaryPayload) {
    this.partitionKey = partitionKey;
    this.rowKey = rowKey;
    this.binaryPayload = binaryPayload;
  }

  private byte[] binaryPayload;

  public byte[] getBinaryPayload() {
    return binaryPayload;
  }

  public void setBinaryPayload(byte[] payload) {
    this.binaryPayload = payload;
  }

}
