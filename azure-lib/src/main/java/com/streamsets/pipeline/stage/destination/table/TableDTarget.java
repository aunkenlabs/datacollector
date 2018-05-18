package com.streamsets.pipeline.stage.destination.table;

import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.lib.table.Groups;
import com.streamsets.pipeline.api.*;

@StageDef(
    version = 1,
    label = "Azure Table",
    description = "Writes data to Azure Tables",
    icon = "azure-table.png",
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class TableDTarget extends DTarget {
  @ConfigDefBean
  public TableConfigBean conf;

  @Override
  protected Target createTarget() {
    return new TableTarget(conf);
  }
}
