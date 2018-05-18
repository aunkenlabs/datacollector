package com.streamsets.pipeline.stage.destination.table;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.table.DataFormatChooserValues;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

public class TableConfigBean {
  @ConfigDefBean(groups = {"TABLE"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JSON",
      label = "Data Format",
      description = "Data Format of the response. Response will be parsed before being placed in the record.",
      displayPosition = 1,
      group = "DATA_FORMAT"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat = DataFormat.JSON;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Storage Account Name",
      description = "Storage Account Name",
      displayPosition = 10,
      group = "TABLE"
  )
  public String accountName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.CREDENTIAL,
      defaultValue = "",
      label = "Storage Account Key",
      description = "Storage Account Key",
      displayPosition = 10,
      group = "TABLE"
  )
  public String accountKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "test",
      label = "Table Name",
      displayPosition = 10,
      elDefs = RecordEL.class,
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      group = "TABLE"
  )
  public String tableName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/id",
      label = "Table PartitionKey",
      description = "Field to use as Table Partition Key",
      displayPosition = 10,
      group = "TABLE"
  )
  public String tablePartitionKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "/timestamp",
      label = "Table RowKey",
      description = "Field to use as Table Row Key",
      displayPosition = 10,
      group = "TABLE"
  )
  public String tableRowKey;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Create Table if missing",
      description = "If set will create a table if it does not exist",
      displayPosition = 10,
      group = "TABLE"
  )
  public Boolean createTableIfNotExists;

  public String getStorageAccountConnectionString() {
    return Utils.format(
        "DefaultEndpointsProtocol=https;" +
            "AccountName={};" +
            "AccountKey={};" +
            "EndpointSuffix=core.windows.net",
        accountName,
        accountKey
    );
  }
}
