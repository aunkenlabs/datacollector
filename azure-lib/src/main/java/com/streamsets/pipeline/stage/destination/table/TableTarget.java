package com.streamsets.pipeline.stage.destination.table;

import com.microsoft.azure.storage.StorageException;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.table.TableCommon;
import com.streamsets.pipeline.lib.table.Errors;
import com.streamsets.pipeline.lib.table.Groups;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

public class TableTarget extends BaseTarget {
  private static final String CHARSET_UTF8 = "UTF-8";
  private static final Logger LOG = LoggerFactory.getLogger(TableTarget.class);

  private TableCommon client;
  private TableConfigBean conf;
  private DataGeneratorFactory generatorFactory;


  public TableTarget(TableConfigBean conf) {
    this.conf = conf;
  }

  private ELVars tableNameVars;

  private ELEval tableNameEval;

  @SuppressWarnings("deprecation")
  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    Target.Context context = getContext();
    tableNameVars = context.createELVars();
    tableNameEval = context.createELEval("tableName");

    try {
      client = new TableCommon(conf.getStorageAccountConnectionString());

    } catch (Exception e) {
      issues.add(getContext().createConfigIssue(
          Groups.TABLE.toString(),
          "Azure Table Connection String",
          Errors.TABLE_COULD_NOT_CONNECT,
          e));
    }

    if (issues.size() == 0) {
      conf.dataGeneratorFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.TABLE.name(),
          "table.",
          issues
      );
      generatorFactory = conf.dataGeneratorFormatConfig.getDataGeneratorFactory();

    }
    return issues;
  }

  @Override
  public void write(Batch batch) throws StageException {
    DataGenerator generator = null;
    OutputStream outputStream = null;

    Iterator<Record> it = batch.getRecords();
    try {
      while (it.hasNext()) {
        Record record = it.next();

        outputStream = new ByteArrayOutputStream();
        generator = generatorFactory.getGenerator(outputStream);
        generator.write(record);
        generator.flush();

        String content = ((ByteArrayOutputStream) outputStream).toString(CHARSET_UTF8);
        writeToTable(record, content);

        IOUtils.closeQuietly(generator);
        IOUtils.closeQuietly(outputStream);
      }

    } catch (StorageException e) {
      LOG.error("Can not write record", e);
      throw new StageException(Errors.TABLE_COULD_NOT_CONNECT, e);

    } catch (Exception e) {
      LOG.error("Can not write record", e);
      throw new StageException(Errors.TABLE_GENERIC, e);

    } finally {
      IOUtils.closeQuietly(generator);
      IOUtils.closeQuietly(outputStream);
    }
  }

  private void writeToTable(Record record, String content) throws
      OnRecordErrorException, ELEvalException, URISyntaxException, StorageException {
    String partitionKey = conf.tablePartitionKey;
    String rowKey = conf.tableRowKey;

    if (!record.has(partitionKey))
      throw new OnRecordErrorException(record, Errors.TABLE_REQUIRED_FIELD, partitionKey);

    // We can use a static value for ROW KEY
    if (rowKey.startsWith("/") && !record.has(rowKey))
      throw new OnRecordErrorException(record, Errors.TABLE_REQUIRED_FIELD, rowKey);

    String tableName = getEvaluatedTableName(record);
    String primaryKeyValue = record.get(partitionKey).getValueAsString();

    String rowKeyValue;
    if (rowKey.startsWith("/"))
      rowKeyValue = record.get(rowKey).getValueAsString();
    else
      rowKeyValue = rowKey;

    if (conf.createTableIfNotExists)
      client.insertAndCreate(tableName, primaryKeyValue, rowKeyValue, content);

    else
      client.insert(tableName, primaryKeyValue, rowKeyValue, content);

  }

  private String getEvaluatedTableName(Record record) throws ELEvalException, OnRecordErrorException {
    RecordEL.setRecordInContext(tableNameVars, record);
    String tableName = tableNameEval.eval(tableNameVars, conf.tableName, String.class);
    if (!tableName.matches("[a-zA-Z0-9]+"))
      throw new OnRecordErrorException(record, Errors.TABLE_INVALID_NAME);

    return tableName;
  }

  @Override
  public void destroy() {
    super.destroy();
  }
}
