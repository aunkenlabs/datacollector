package com.streamsets.pipeline.stage.destination.table;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTableTarget {
//  @Test
//  TODO: Ask for suggestions on how to mock Azure SDK for testing
  public void testConfig() throws Exception {
    TableConfigBean conf = new TableConfigBean();
    conf.accountName = "test";
    conf.accountKey = "QG9EWyEvI77K";
    conf.tableName = "test";
    conf.tablePartitionKey = "/id";
    conf.tableRowKey = "/timestamp";
    conf.createTableIfNotExists = false;
    conf.dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    conf.dataGeneratorFormatConfig.textFieldPath = "/payload";
    conf.dataFormat = DataFormat.TEXT;


    TableTarget target = new TableTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(TableDTarget.class, target).build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();

    runner.runInit();

    Record record = RecordCreator.create();
    Map<String, Field> fields = new HashMap<>();
    fields.put("id", Field.create("123456"));
    fields.put("timestamp", Field.create(System.currentTimeMillis()));
    fields.put("payload", Field.create("{\"hello\":\"world\"}"));
    record.set(Field.create(fields));

    runner.runWrite(Arrays.asList(record));

//    Assert.assertNotNull(events);
//    Assert.assertEquals(1, events.size());
//    Assert.assertEquals(Field.Type.STRING, events.get(0).get().getType());
//    Assert.assertEquals("Catch them all!", events.get(0).get().getValueAsString());
    runner.runDestroy();
  }
}
