/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.execution.runner.common;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.streamsets.datacollector.config.DeliveryGuarantee;
import com.streamsets.datacollector.config.MemoryLimitConfiguration;
import com.streamsets.datacollector.config.MemoryLimitExceeded;
import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.metrics.MetricsConfigurator;
import com.streamsets.datacollector.record.HeaderImpl;
import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.restapi.bean.CounterJson;
import com.streamsets.datacollector.restapi.bean.HistogramJson;
import com.streamsets.datacollector.restapi.bean.MeterJson;
import com.streamsets.datacollector.restapi.bean.MetricRegistryJson;
import com.streamsets.datacollector.runner.BatchContextImpl;
import com.streamsets.datacollector.runner.BatchListener;
import com.streamsets.datacollector.runner.ErrorSink;
import com.streamsets.datacollector.runner.FullPipeBatch;
import com.streamsets.datacollector.runner.Observer;
import com.streamsets.datacollector.runner.Pipe;
import com.streamsets.datacollector.runner.PipeBatch;
import com.streamsets.datacollector.runner.PipeContext;
import com.streamsets.datacollector.runner.PipelineRunner;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.PushSourceContextDelegate;
import com.streamsets.datacollector.runner.RunnerPool;
import com.streamsets.datacollector.runner.SourceOffsetTracker;
import com.streamsets.datacollector.runner.SourcePipe;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.runner.StagePipe;
import com.streamsets.datacollector.runner.production.BadRecordsHandler;
import com.streamsets.datacollector.runner.production.PipelineErrorNotificationRequest;
import com.streamsets.datacollector.runner.production.StatsAggregationHandler;
import com.streamsets.datacollector.util.AggregatorUtil;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.ErrorListener;
import com.streamsets.pipeline.api.OffsetCommitTrigger;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.ErrorMessage;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;


public class ProductionPipelineRunner implements PipelineRunner, PushSourceContextDelegate {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionPipelineRunner.class);

  private final RuntimeInfo runtimeInfo;
  private final com.streamsets.datacollector.util.Configuration configuration;
  private final MetricRegistry metrics;
  private SourceOffsetTracker offsetTracker;
  private final SnapshotStore snapshotStore;
  private String sourceOffset;
  private String newSourceOffset;
  private DeliveryGuarantee deliveryGuarantee;
  private final String pipelineName;
  private final String revision;
  private final List<ErrorListener> errorListeners;

  private SourcePipe originPipe;
  private List<List<Pipe>> pipes;
  private RunnerPool<List<Pipe>> runnerPool;
  private BadRecordsHandler badRecordsHandler;
  private StatsAggregationHandler statsAggregationHandler;

  private final Timer batchProcessingTimer;
  private final Meter batchCountMeter;
  private final Counter batchCountCounter;
  private final Histogram batchInputRecordsHistogram;
  private final Histogram batchOutputRecordsHistogram;
  private final Histogram batchErrorRecordsHistogram;
  private final Histogram batchErrorsHistogram;
  private final Meter batchInputRecordsMeter;
  private final Meter batchOutputRecordsMeter;
  private final Meter batchErrorRecordsMeter;
  private final Meter batchErrorMessagesMeter;
  private final Counter batchInputRecordsCounter;
  private final Counter batchOutputRecordsCounter;
  private final Counter batchErrorRecordsCounter;
  private final Counter batchErrorMessagesCounter;
  private final Counter memoryConsumedCounter;
  private MetricRegistryJson metricRegistryJson;
  private Long rateLimit;

  private RateLimiter rateLimiter;

  /*indicates if the execution must be stopped after the current batch*/
  private volatile boolean stop = false;
  /*indicates if the next batch of data should be captured, only the next batch*/
  private volatile int batchesToCapture = 0;
  /*indicates the snapshot name to be captured*/
  private volatile String snapshotName;
  /*indicates the batch size to be captured*/
  private volatile int snapshotBatchSize;
  /*Cache last N error records per stage in memory*/
  private final Map<String, EvictingQueue<Record>> stageToErrorRecordsMap;
  /*Cache last N error messages in memory*/
  private final Map<String, EvictingQueue<ErrorMessage>> stageToErrorMessagesMap;
  /**/
  private BlockingQueue<Object> observeRequests;
  private Observer observer;
  private BlockingQueue<Record> statsAggregatorRequests;
  private final List<BatchListener> batchListenerList = new CopyOnWriteArrayList<>();
  private final Object errorRecordsMutex;
  private MemoryLimitConfiguration memoryLimitConfiguration;
  private long lastMemoryLimitNotification;
  private ThreadHealthReporter threadHealthReporter;
  private final List<List<StageOutput>> capturedBatches = new ArrayList<>();
  private PipeContext pipeContext = null;

  @Inject
  public ProductionPipelineRunner(@Named("name") String pipelineName, @Named("rev") String revision,
                                  Configuration configuration,
                                  RuntimeInfo runtimeInfo, MetricRegistry metrics, SnapshotStore snapshotStore,
                                  ThreadHealthReporter threadHealthReporter) {
    this.runtimeInfo = runtimeInfo;
    this.configuration = configuration;
    this.metrics = metrics;
    this.threadHealthReporter = threadHealthReporter;
    this.snapshotStore = snapshotStore;
    this.pipelineName = pipelineName;
    this.revision = revision;
    stageToErrorRecordsMap = new HashMap<>();
    stageToErrorMessagesMap = new HashMap<>();
    errorRecordsMutex = new Object();
    this.errorListeners = new ArrayList<>();

    MetricsConfigurator.registerPipeline(pipelineName, revision);
    batchProcessingTimer = MetricsConfigurator.createTimer(metrics, "pipeline.batchProcessing", pipelineName, revision);
    batchCountMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchCount", pipelineName, revision);
    batchCountCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchCount", pipelineName, revision);
    batchInputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.inputRecordsPerBatch",
      pipelineName, revision);
    batchOutputRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.outputRecordsPerBatch",
      pipelineName, revision);
    batchErrorRecordsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.errorRecordsPerBatch",
      pipelineName, revision);
    batchErrorsHistogram = MetricsConfigurator.createHistogram5Min(metrics, "pipeline.errorsPerBatch", pipelineName,
      revision);
    batchInputRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchInputRecords", pipelineName,
      revision);
    batchOutputRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchOutputRecords", pipelineName,
      revision);
    batchErrorRecordsMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchErrorRecords", pipelineName,
      revision);
    batchErrorMessagesMeter = MetricsConfigurator.createMeter(metrics, "pipeline.batchErrorMessages", pipelineName,
      revision);
    batchInputRecordsCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchInputRecords", pipelineName,
      revision);
    batchOutputRecordsCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchOutputRecords", pipelineName,
      revision);
    batchErrorRecordsCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchErrorRecords", pipelineName,
      revision);
    batchErrorMessagesCounter = MetricsConfigurator.createCounter(metrics, "pipeline.batchErrorMessages", pipelineName,
      revision);
    memoryConsumedCounter = MetricsConfigurator.createCounter(metrics, "pipeline.memoryConsumed", pipelineName,
      revision);
  }

  public void setObserveRequests(BlockingQueue<Object> observeRequests) {
    this.observeRequests = observeRequests;
  }

  public void setStatsAggregatorRequests(BlockingQueue<Record> statsAggregatorRequests) {
    this.statsAggregatorRequests = statsAggregatorRequests;
  }

  public void addErrorListeners(List<ErrorListener> errorListeners) {
    LOG.info("Adding error listeners" + errorListeners.size());
    this.errorListeners.addAll(errorListeners);
  }

  public void updateMetrics(MetricRegistryJson metricRegistryJson) {
    this.metricRegistryJson = metricRegistryJson;
    HistogramJson inputHistogramJson = metricRegistryJson.getHistograms().get("pipeline.inputRecordsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchInputRecordsHistogram.update(inputHistogramJson.getCount());
    HistogramJson outputHistogramJson = metricRegistryJson.getHistograms().get("pipeline.outputRecordsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchOutputRecordsHistogram.update(outputHistogramJson.getCount());
    HistogramJson errorHistogramJson = metricRegistryJson.getHistograms().get("pipeline.errorRecordsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchErrorRecordsHistogram.update(errorHistogramJson.getCount());
    HistogramJson errorPerBatchHistogramJson = metricRegistryJson.getHistograms().get("pipeline.errorsPerBatch" + MetricsConfigurator.HISTOGRAM_M5_SUFFIX);
    batchErrorsHistogram.update(errorPerBatchHistogramJson.getCount());
    MeterJson batchInputRecords = metricRegistryJson.getMeters().get("pipeline.batchInputRecords" + MetricsConfigurator.METER_SUFFIX);
    batchInputRecordsMeter.mark(batchInputRecords.getCount());
    batchInputRecordsCounter.inc(batchInputRecords.getCount());
    MeterJson batchOutputRecords = metricRegistryJson.getMeters().get("pipeline.batchOutputRecords" + MetricsConfigurator.METER_SUFFIX);
    batchOutputRecordsMeter.mark(batchOutputRecords.getCount());
    batchOutputRecordsCounter.inc(batchOutputRecords.getCount());
    MeterJson batchErrorRecords = metricRegistryJson.getMeters().get("pipeline.batchErrorRecords" + MetricsConfigurator.METER_SUFFIX);
    batchErrorRecordsMeter.mark(batchErrorRecords.getCount());
    batchErrorRecordsCounter.inc(batchErrorRecords.getCount());
    MeterJson batchErrorMessagesRecords = metricRegistryJson.getMeters().get("pipeline.batchErrorMessages" + MetricsConfigurator.METER_SUFFIX);
    batchErrorMessagesMeter.mark(batchErrorMessagesRecords.getCount());
    batchErrorMessagesCounter.inc(batchErrorMessagesRecords.getCount());
    CounterJson memoryConsumer = metricRegistryJson.getCounters().get("pipeline.memoryConsumed" + MetricsConfigurator.COUNTER_SUFFIX);
    memoryConsumedCounter.inc(memoryConsumer.getCount());
  }

  @Override
  public MetricRegistryJson getMetricRegistryJson() {
    return this.metricRegistryJson;
  }

  public void setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
    this.deliveryGuarantee = deliveryGuarantee;
  }

  public void setMemoryLimitConfiguration(MemoryLimitConfiguration memoryLimitConfiguration) {
    this.memoryLimitConfiguration = memoryLimitConfiguration;
  }

  public void setRateLimit(Long rateLimit) {
    this.rateLimit = rateLimit;
    rateLimiter = RateLimiter.create(rateLimit.doubleValue());
  }

  public void setOffsetTracker(SourceOffsetTracker offsetTracker) {
    this.offsetTracker = offsetTracker;
  }

  public void setThreadHealthReporter(ThreadHealthReporter threadHealthReporter) {
    this.threadHealthReporter = threadHealthReporter;
  }

  @Override
  public void setObserver(Observer observer) {
    this.observer = observer;
  }

  @Override
  public RuntimeInfo getRuntimeInfo() {
    return runtimeInfo;
  }

  @Override
  public boolean isPreview() {
    return false;
  }

  @Override
  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  public void run(
    SourcePipe originPipe,
    List<List<Pipe>> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    this.originPipe = originPipe;
    this.pipes = pipes;
    this.badRecordsHandler = badRecordsHandler;
    this.statsAggregationHandler = statsAggregationHandler;
    this.runnerPool = new RunnerPool<>(pipes);

    if(originPipe.getStage().getStage() instanceof PushSource) {
      runPushSource();
    } else {
      runPollSource();
    }
  }

  private void runPushSource() throws StageException, PipelineRuntimeException {
    // This object will receive delegated calls from the push origin callbacks
    originPipe.getStage().setPushSourceContextDelegate(this);

    // Configured maximal batch size
    int batchSize = configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT);

    // Push origin will block on the call until the either all data have been consumed or the pipeline stopped
    originPipe.process(offsetTracker.getOffsets(), batchSize);
  }

  private FullPipeBatch createFullPipeBatch(String previousOffset) {
    FullPipeBatch pipeBatch;
    if(batchesToCapture > 0) {
      pipeBatch = new FullPipeBatch(previousOffset, snapshotBatchSize, true);
    } else {
      pipeBatch = new FullPipeBatch(previousOffset, configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT), false);
    }
    pipeBatch.setRateLimiter(rateLimiter);

    return pipeBatch;
  }

  @Override
  public BatchContext startBatch() {
    // Pick up any recent changes done to the rule definitions
    if(observer != null) {
      observer.reconfigure();
    }

    FullPipeBatch pipeBatch = createFullPipeBatch(null);
    BatchContextImpl batchContext = new BatchContextImpl(pipeBatch);

    originPipe.prepareBatchContext(batchContext);

    return batchContext;
  }

  @Override
  public boolean processBatch(BatchContext batchCtx, String entity, String offset) {
    BatchContextImpl batchContext = (BatchContextImpl) batchCtx;

    Map<String, Long> memoryConsumedByStage = new HashMap<>();
    Map<String, Object> stageBatchMetrics = new HashMap<>();

    Map<String, Object> batchMetrics = originPipe.finishBatchContext(batchContext);

    if (isStatsAggregationEnabled()) {
      stageBatchMetrics.put(originPipe.getStage().getInfo().getInstanceName(), batchMetrics);
    }

    try {
      runSourceLessBatch(
        batchContext.getStartTime(),
        batchContext.getPipeBatch(),
        entity,
        offset,
        memoryConsumedByStage,
        stageBatchMetrics
      );
    } catch (PipelineException|StageException e) {
      LOG.error("Can't process batch", e);
      return false;
    }

    return true;
  }

  @Override
  public void commitOffset(String entity, String offset) {
    offsetTracker.commitOffset(entity, offset);
  }

  public void runPollSource() throws StageException, PipelineRuntimeException {
    while (!offsetTracker.isFinished() && !stop) {
      if (threadHealthReporter != null) {
        threadHealthReporter.reportHealth(ProductionPipelineRunnable.RUNNABLE_NAME, -1, System.currentTimeMillis());
      }
      try {
        for (BatchListener batchListener : batchListenerList) {
          batchListener.preBatch();
        }

        if(observer != null) {
          observer.reconfigure();
        }

        // Start of the batch execution
        long start = System.currentTimeMillis();
        FullPipeBatch pipeBatch = createFullPipeBatch(offsetTracker.getOffsets().get(Source.POLL_SOURCE_OFFSET_KEY));

        // Run origin
        Map<String, Long> memoryConsumedByStage = new HashMap<>();
        Map<String, Object> stageBatchMetrics = new HashMap<>();
        processPipe(
          originPipe,
          pipeBatch,
          false,
          null,
          null,
          memoryConsumedByStage,
          stageBatchMetrics
        );

        // Since the origin already run, the FullPipeBatch will have a new offset
        String newOffset = pipeBatch.getNewOffset();

        // Run rest of the pipeline
        runSourceLessBatch(
          start,
          pipeBatch,
          Source.POLL_SOURCE_OFFSET_KEY,
          newOffset,
          memoryConsumedByStage,
          stageBatchMetrics
        );

        for (BatchListener batchListener : batchListenerList) {
          batchListener.postBatch();
        }
      } catch (Throwable throwable) {
        sendPipelineErrorNotificationRequest(throwable);
        errorNotification(originPipe, pipes, throwable);
        Throwables.propagateIfInstanceOf(throwable, StageException.class);
        Throwables.propagateIfInstanceOf(throwable, PipelineRuntimeException.class);
        Throwables.propagate(throwable);
      }
    }
  }

  @Override
  public void errorNotification(SourcePipe originPipe, List<List<Pipe>> pipes, Throwable throwable) {
    Set<ErrorListener> listeners = Sets.newIdentityHashSet();
    for (BatchListener batchListener : batchListenerList) {
      batchListener.postBatch();
    }
    listeners.addAll(new ArrayList<>(errorListeners));
    if (originPipe.getStage().getStage() instanceof ErrorListener) {
      listeners.add((ErrorListener) originPipe.getStage().getStage());
    }
    for(List<Pipe> pipeRunner : pipes) {
      for (Pipe pipe : pipeRunner) {
        Stage stage = pipe.getStage().getStage();
        if (stage instanceof ErrorListener) {
          listeners.add((ErrorListener) stage);
        }
      }
    }
    for (ErrorListener listener : listeners) {
      try {
        listener.errorNotification(throwable);
      } catch (Exception ex) {
        String msg = Utils.format("Error in calling ErrorListenerStage {}: {}", listener.getClass().getName(), ex);
        LOG.error(msg, ex);
      }
    }
  }

  @Override
  public void run(
    SourcePipe originPipe,
    List<List<Pipe>> pipes,
    BadRecordsHandler badRecordsHandler,
    List<StageOutput> stageOutputsToOverride,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    throw new UnsupportedOperationException();
  }

  /**
   * Since stages are allowed to produce events during destroy() phase, we handle the destroy event as simplified
   * runBatch. We go over all the StagePipes and if it's on data path we destroy it immediately, if it's on  event path, we
   * run it one more time. Since the stages are sorted we know that destroyed stage will never be needed again. Non
   * stage pipes are always processed to generate required structures in PipeBatch.
   */
  @Override
  public void destroy(
    SourcePipe originPipe,
    List<List<Pipe>> pipes,
    BadRecordsHandler badRecordsHandler,
    StatsAggregationHandler statsAggregationHandler
  ) throws StageException, PipelineRuntimeException {
    int batchSize = configuration.get(Constants.MAX_BATCH_SIZE_KEY, Constants.MAX_BATCH_SIZE_DEFAULT);
    FullPipeBatch pipeBatch;

    // Destroy origin pipe
    pipeBatch = new FullPipeBatch(null, batchSize, false);
    try {
      LOG.trace("Destroying origin pipe");
      originPipe.destroy(pipeBatch);
    } catch(RuntimeException e) {
      LOG.warn("Exception throw while destroying pipe", e);
    }

    // Now destroy the pipe runners
    for(List<Pipe> pipeRunner : pipes) {
      pipeBatch.skipStage(originPipe);
      destroyPipes(pipeRunner, pipeBatch, badRecordsHandler);

      // Next iteration should have new and empty PipeBatch
      pipeBatch = new FullPipeBatch(null, batchSize, false);
    }
  }

  private void destroyPipes(
    List<Pipe> pipes,
    FullPipeBatch pipeBatch,
    BadRecordsHandler badRecordsHandler
  ) throws PipelineRuntimeException, StageException {
    long lastBatchTime = offsetTracker.getLastBatchTime();
    for (Pipe pipe : pipes) {
      // Set the last batch time in the stage context of each pipe
      ((StageContext)pipe.getStage().getContext()).setLastBatchTime(lastBatchTime);
      String instanceName = pipe.getStage().getConfiguration().getInstanceName();

      if(pipe instanceof StagePipe) {
        // Stage pipes are processed only if they are in event path
        if(pipe.getStage().getConfiguration().isInEventPath()) {
          LOG.trace("Stage pipe {} is in event path, running last process", instanceName);
          pipe.process(pipeBatch);
        } else {
          LOG.trace("Stage pipe {} is in data path, skipping it's processing.", instanceName);
          pipeBatch.skipStage(pipe);
        }
      } else {
        // Non stage pipes are executed always
        LOG.trace("Non stage pipe {}, running last process", instanceName);
        pipe.process(pipeBatch);
      }

      // And finally destroy the pipe
      try {
        LOG.trace("Running destroy for {}", instanceName);
        pipe.destroy(pipeBatch);
      } catch(RuntimeException e) {
        LOG.warn("Exception throw while destroying pipe", e);
      }
    }
    badRecordsHandler.handle(newSourceOffset, getBadRecords(pipeBatch.getErrorSink()));
  }

  @Override
  public List<List<StageOutput>> getBatchesOutput() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getSourceOffset() {
    return sourceOffset;
  }

  @Override
  public String getNewSourceOffset() {
    return newSourceOffset;
  }

  public String getCommittedOffset() {
    return offsetTracker.getOffset();
  }

  /**
   * Stops execution of the pipeline after the current batch completes
   */
  public void stop() throws PipelineException {
    this.stop = true;
    if(batchesToCapture > 0) {
      cancelSnapshot(this.snapshotName);
      snapshotStore.deleteSnapshot(pipelineName, revision, snapshotName);
    }
  }

  public boolean wasStopped() {
    return stop;
  }

  public void capture(String snapshotName, int batchSize, int batches) {
    Preconditions.checkArgument(batchSize > 0);
    this.snapshotName = snapshotName;
    this.snapshotBatchSize = batchSize;
    this.batchesToCapture = batches;
  }

  public void cancelSnapshot(String snapshotName) throws PipelineException {
    Preconditions.checkArgument(this.snapshotName != null && this.snapshotName.equals(snapshotName));
    synchronized (this) {
      this.snapshotBatchSize = 0;
      this.batchesToCapture = 0;
      capturedBatches.clear();
    }
  }

  private boolean processPipe(
    Pipe pipe,
    PipeBatch pipeBatch,
    boolean committed,
    String entityName,
    String newOffset,
    Map<String, Long> memoryConsumedByStage,
    Map<String, Object> stageBatchMetrics
  ) throws PipelineRuntimeException, StageException {

    // Set the last batch time in the stage context of each pipe
    ((StageContext)pipe.getStage().getContext()).setLastBatchTime(offsetTracker.getLastBatchTime());

    if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE
        && pipe.getStage().getDefinition().getType() == StageType.TARGET
        && !committed
      ) {
      // target cannot control offset commit in AT_MOST_ONCE mode
      offsetTracker.commitOffset(entityName, newOffset);
      committed = true;
    }
    pipe.process(pipeBatch);
    if (pipe instanceof StagePipe) {
      memoryConsumedByStage.put(pipe.getStage().getInfo().getInstanceName(), ((StagePipe)pipe).getMemoryConsumed());
      if (isStatsAggregationEnabled()) {
        stageBatchMetrics.put(pipe.getStage().getInfo().getInstanceName(), ((StagePipe) pipe).getBatchMetrics());
      }
    }

    return committed;
  }

  private void runSourceLessBatch(
    long start,
    FullPipeBatch pipeBatch,
    String entityName,
    String newOffset,
    Map<String, Long> memoryConsumedByStage,
    Map<String, Object> stageBatchMetrics
  ) throws PipelineException, StageException {
    boolean committed = false;
    OffsetCommitTrigger offsetCommitTrigger = getOffsetCommitTrigger(originPipe, pipes);
    sourceOffset = pipeBatch.getPreviousOffset();

    List<Pipe> runnerPipes = null;
    try {
      runnerPipes = runnerPool.getRunner();
      for (Pipe pipe : runnerPipes) {
        committed = processPipe(pipe, pipeBatch, committed, entityName, newOffset, memoryConsumedByStage, stageBatchMetrics);
      }
    } finally {
      if(runnerPipes != null) {
        runnerPool.returnRunner(runnerPipes);
      }
    }

    enforceMemoryLimit(memoryConsumedByStage);
    badRecordsHandler.handle(newSourceOffset, getBadRecords(pipeBatch.getErrorSink()));
    if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
      // When AT_LEAST_ONCE commit only if
      // 1. There is no offset commit trigger for this pipeline or
      // 2. there is a commit trigger and it is on
      if (offsetCommitTrigger == null || offsetCommitTrigger.commit()) {
        offsetTracker.commitOffset(entityName, newOffset);
      }
    }

    long batchDuration = System.currentTimeMillis() - start;
    batchProcessingTimer.update(batchDuration, TimeUnit.MILLISECONDS);
    batchCountCounter.inc();
    batchCountMeter.mark();
    batchInputRecordsHistogram.update(pipeBatch.getInputRecords());
    batchOutputRecordsHistogram.update(pipeBatch.getOutputRecords());
    batchErrorRecordsHistogram.update(pipeBatch.getErrorRecords());
    batchErrorsHistogram.update(pipeBatch.getErrorMessages());
    batchInputRecordsMeter.mark(pipeBatch.getInputRecords());
    batchOutputRecordsMeter.mark(pipeBatch.getOutputRecords());
    batchErrorRecordsMeter.mark(pipeBatch.getErrorRecords());
    batchErrorMessagesMeter.mark(pipeBatch.getErrorMessages());
    batchInputRecordsCounter.inc(pipeBatch.getInputRecords());
    batchOutputRecordsCounter.inc(pipeBatch.getOutputRecords());
    batchErrorRecordsCounter.inc(pipeBatch.getErrorRecords());
    batchErrorMessagesCounter.inc(pipeBatch.getErrorMessages());

    if (pipeContext != null) {
      pipeContext.getRuntimeStats().setLastBatchInputRecordsCount(pipeBatch.getInputRecords());
      pipeContext.getRuntimeStats().setLastBatchOutputRecordsCount((pipeBatch.getOutputRecords()));
      pipeContext.getRuntimeStats().setLastBatchErrorRecordsCount(pipeBatch.getErrorRecords());
      pipeContext.getRuntimeStats().setLastBatchErrorMessagesCount(pipeBatch.getErrorMessages());
    }

    if (isStatsAggregationEnabled()) {
      Map<String, Object> pipelineBatchMetrics = new HashMap<>();
      pipelineBatchMetrics.put(AggregatorUtil.PIPELINE_BATCH_DURATION, batchDuration);
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_COUNT, 1);
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_INPUT_RECORDS, pipeBatch.getInputRecords());
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_OUTPUT_RECORDS, pipeBatch.getOutputRecords());
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_ERROR_RECORDS, pipeBatch.getErrorRecords());
      pipelineBatchMetrics.put(AggregatorUtil.BATCH_ERRORS, pipeBatch.getErrorMessages());
      pipelineBatchMetrics.put(AggregatorUtil.STAGE_BATCH_METRICS, stageBatchMetrics);

      AggregatorUtil.enqueStatsRecord(
          AggregatorUtil.createMetricRecord(pipelineBatchMetrics),
          statsAggregatorRequests,
          configuration
      );
    }

    newSourceOffset = newOffset;

    synchronized (this) {
      if( batchesToCapture > 0 && pipeBatch.getSnapshotsOfAllStagesOutput() != null) {
        List<StageOutput> snapshot = pipeBatch.getSnapshotsOfAllStagesOutput();
        if (!snapshot.isEmpty()) {
          capturedBatches.add(snapshot);
        }
        /*
         * Reset the capture snapshot variable only after capturing the snapshot
         * This guarantees that once captureSnapshot is called, the output is captured exactly once
         * */
        batchesToCapture--;
        if (batchesToCapture == 0) {
          snapshotBatchSize = 0;
          batchesToCapture = 0;
          if (!capturedBatches.isEmpty()) {
            snapshotStore.save(pipelineName, revision, snapshotName, capturedBatches);
            capturedBatches.clear();
          }
        }
      }
    }

    // Retain X number of error records per stage
    Map<String, List<Record>> errorRecords = pipeBatch.getErrorSink().getErrorRecords();
    Map<String, List<ErrorMessage>> errorMessages = pipeBatch.getErrorSink().getStageErrors();
    retainErrorsInMemory(errorRecords, errorMessages);

    // Write Pipeline data rule and drift rule results to aggregator target
    if (isStatsAggregationEnabled()) {
      List<Record> stats = new ArrayList<>();
      statsAggregatorRequests.drainTo(stats);
      statsAggregationHandler.handle(sourceOffset, stats);
    }
  }

  private RecordImpl getSourceRecord(Record record) {
    return (RecordImpl) ((RecordImpl)record).getHeader().getSourceRecord();
  }

  private void injectErrorInfo(RecordImpl sourceRecord, Record record) {
    HeaderImpl header = sourceRecord.getHeader();
    header.copyErrorFrom(record);
    header.setErrorContext(runtimeInfo.getId(), pipelineName);
  }

  private void enforceMemoryLimit(Map<String, Long> memoryConsumedByStage) throws PipelineRuntimeException {
    long totalMemoryConsumed = 0;
    for(Map.Entry<String, Long> entry : memoryConsumedByStage.entrySet()) {
      totalMemoryConsumed += entry.getValue();
    }
    memoryConsumedCounter.inc(totalMemoryConsumed - memoryConsumedCounter.getCount());
    long memoryLimit = memoryLimitConfiguration.getMemoryLimit();
    if (memoryLimit > 0 && totalMemoryConsumed > memoryLimit) {
      String largestConsumer = "unknown";
      long largestConsumed = 0;
      Map<String, String> humanReadbleMemoryConsumptionByStage = new HashMap<>();
      for(Map.Entry<String, Long> entry : memoryConsumedByStage.entrySet()) {
        if (entry.getValue() > largestConsumed) {
          largestConsumed = entry.getValue();
          largestConsumer = entry.getKey();
        }
        humanReadbleMemoryConsumptionByStage.put(entry.getKey(), entry.getValue() + " MB");
      }
      humanReadbleMemoryConsumptionByStage.remove(largestConsumer);
      PipelineRuntimeException ex = new PipelineRuntimeException(ContainerError.CONTAINER_0011, totalMemoryConsumed,
        memoryLimit, largestConsumer, largestConsumed, humanReadbleMemoryConsumptionByStage);
      String msg = "Pipeline memory limit exceeded: " + ex;
      long elapsedTimeSinceLastTriggerMins = TimeUnit.MILLISECONDS.
        toMinutes(System.currentTimeMillis() - lastMemoryLimitNotification);
      lastMemoryLimitNotification = System.currentTimeMillis();
      if (elapsedTimeSinceLastTriggerMins < 60) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Memory limit has been triggered, will take {} in {} mins",
            memoryLimitConfiguration.getMemoryLimitExceeded(), (60 - elapsedTimeSinceLastTriggerMins));
        }
      } else if (memoryLimitConfiguration.getMemoryLimitExceeded() == MemoryLimitExceeded.LOG) {
        LOG.error(msg, ex);
      } else if (memoryLimitConfiguration.getMemoryLimitExceeded() == MemoryLimitExceeded.ALERT) {
        LOG.error(msg, ex);
        sendPipelineErrorNotificationRequest(ex);
      } else if (memoryLimitConfiguration.getMemoryLimitExceeded() == MemoryLimitExceeded.STOP_PIPELINE) {
        throw ex;
      }
    }
  }
  private void sendPipelineErrorNotificationRequest(Throwable throwable) {
    boolean offered = false;
    try {
      observeRequests.put(new PipelineErrorNotificationRequest(throwable));
      offered = true;
    } catch (InterruptedException e) {
    }
    if(!offered) {
      LOG.error("Could not submit alert request for pipeline ending error: " + throwable, throwable);
    }
  }
  private List<Record> getBadRecords(ErrorSink errorSink) throws PipelineRuntimeException {
    List<Record> badRecords = new ArrayList<>();
    for (Map.Entry<String, List<Record>> entry : errorSink.getErrorRecords().entrySet()) {
      for (Record record : entry.getValue()) {
        RecordImpl sourceRecord = getSourceRecord(record);
        injectErrorInfo(sourceRecord, record);
        badRecords.add(sourceRecord);
      }
    }
    return badRecords;
  }

  public SourceOffsetTracker getOffSetTracker() {
    return this.offsetTracker;
  }

  private void retainErrorsInMemory(Map<String, List<Record>> errorRecords, Map<String,
    List<ErrorMessage>> errorMessages) {
    synchronized (errorRecordsMutex) {
      for (Map.Entry<String, List<Record>> e : errorRecords.entrySet()) {
        EvictingQueue<Record> errorRecordList = stageToErrorRecordsMap.get(e.getKey());
        if (errorRecordList == null) {
          //replace with a data structure with an upper cap
          errorRecordList = EvictingQueue.create(configuration.get(Constants.MAX_ERROR_RECORDS_PER_STAGE_KEY,
            Constants.MAX_ERROR_RECORDS_PER_STAGE_DEFAULT));
          stageToErrorRecordsMap.put(e.getKey(), errorRecordList);
        }
        errorRecordList.addAll(errorRecords.get(e.getKey()));
      }
      for (Map.Entry<String, List<ErrorMessage>> e : errorMessages.entrySet()) {
        EvictingQueue<ErrorMessage> errorMessageList = stageToErrorMessagesMap.get(e.getKey());
        if (errorMessageList == null) {
          //replace with a data structure with an upper cap
          errorMessageList = EvictingQueue.create(configuration.get(Constants.MAX_PIPELINE_ERRORS_KEY,
            Constants.MAX_PIPELINE_ERRORS_DEFAULT));
          stageToErrorMessagesMap.put(e.getKey(), errorMessageList);
        }
        errorMessageList.addAll(errorMessages.get(e.getKey()));
      }
    }
  }

  @SuppressWarnings("unchecked")
  public List<Record> getErrorRecords(String instanceName, int size) {
    synchronized (errorRecordsMutex) {
      if (stageToErrorRecordsMap == null || stageToErrorRecordsMap.isEmpty()
        || stageToErrorRecordsMap.get(instanceName) == null || stageToErrorRecordsMap.get(instanceName).isEmpty()) {
        return Collections.emptyList();
      }
      if (stageToErrorRecordsMap.get(instanceName).size() > size) {
        return new CopyOnWriteArrayList<>(stageToErrorRecordsMap.get(instanceName)).subList(0, size);
      } else {
        return new CopyOnWriteArrayList<>(stageToErrorRecordsMap.get(instanceName));
      }
    }
  }

  public List<ErrorMessage> getErrorMessages(String instanceName, int size) {
    synchronized (errorRecordsMutex) {
      if (stageToErrorMessagesMap == null || stageToErrorMessagesMap.isEmpty()
        || stageToErrorMessagesMap.get(instanceName) == null || stageToErrorMessagesMap.get(instanceName).isEmpty()) {
        return Collections.emptyList();
      }
      if (stageToErrorMessagesMap.get(instanceName).size() > size) {
        return new CopyOnWriteArrayList<>(stageToErrorMessagesMap.get(instanceName)).subList(0, size);
      } else {
        return new CopyOnWriteArrayList<>(stageToErrorMessagesMap.get(instanceName));
      }
    }
  }

  @Override
  public void registerListener(BatchListener batchListener) {
    batchListenerList.add(batchListener);
  }

  private boolean isStatsAggregationEnabled() {
    return null != statsAggregatorRequests;
  }

  // TODO: Do we need to keep list of all offset committing stages?
  private OffsetCommitTrigger getOffsetCommitTrigger(SourcePipe originPipe, List<List<Pipe>> pipes) {
    // Origin can't be OffsetCommitTrigger, so going only through out the pipe list and only through first instance
    // as we're interested in the first offset committing stage at this point.
    for (Pipe pipe : pipes.get(0)) {
      Stage stage = pipe.getStage().getStage();
      if (stage instanceof Target &&
        stage instanceof OffsetCommitTrigger) {
        return (OffsetCommitTrigger) stage;
      }
    }
    return null;
  }

  public void setPipeContext(PipeContext pipeContext) {
    this.pipeContext = pipeContext;
  }

}
