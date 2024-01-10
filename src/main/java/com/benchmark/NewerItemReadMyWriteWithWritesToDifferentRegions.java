package com.benchmark;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDiagnosticsHandler;
import com.azure.cosmos.CosmosDiagnosticsThresholds;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosRegionSwitchHint;
import com.azure.cosmos.SessionRetryOptionsBuilder;
import com.azure.cosmos.implementation.CosmosDaemonThreadFactory;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.fail;

public class NewerItemReadMyWriteWithWritesToDifferentRegions extends Workload {
    private static final Logger logger = LoggerFactory.getLogger(RandomItemReadMyWriteWithWritesToDifferentRegions.class);
    private static ScheduledThreadPoolExecutor executorForWritesAgainstFirstPreferredRegion;
    private static ScheduledThreadPoolExecutor executorForReadsAgainstFirstPreferredRegion;
    private static ScheduledThreadPoolExecutor executorForWritesAgainstOtherPreferredRegions;
    private static ScheduledFuture<?> scheduledFutureForWriteAgainstFirstPreferredRegion;
    private static ScheduledFuture<?> scheduledFutureForReadAgainstFirstPreferredRegion;
    private static ScheduledFuture<?>[] scheduledFuturesForWriteAgainstOtherPreferredRegions
            = new ScheduledFuture[50];
    private static CosmosAsyncClient client;
    private static CosmosAsyncContainer container;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(
            JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),
            true
    );
    private static final AtomicBoolean shouldCleanUp = new AtomicBoolean(true);
    private static final LinkedList<String> globalCache = new LinkedList<>();
    @Override
    public void execute(Configuration cfg) {

        String runId = UUID.randomUUID().toString();
        logger.info("Starting run with runId : {}", runId);

        int loopCount = computeLoopCount(cfg.getRunDuration());
        logger.info("{} loops worth 10s to run...", loopCount);

        int satelliteRegionWriterThreadCount = Math.min(cfg.getSatelliteRegionWriterThreadCount(), 20);
        AtomicBoolean shouldStop = new AtomicBoolean(false);

        AtomicInteger totalWriteCountFromPrimaryWriter = new AtomicInteger(0);
        AtomicInteger totalWriteCountFromSecondaryWriter = new AtomicInteger(0);
        AtomicInteger totalReadCountFromPrimaryReader = new AtomicInteger(0);
        AtomicInteger totalSuccessfulReadCountFromPrimaryReader = new AtomicInteger(0);
        AtomicInteger totalSuccessfulWriteAttemptsFromPrimaryWriter = new AtomicInteger(0);
        AtomicInteger totalSuccessfulWriteAttemptsFromSecondaryWriter = new AtomicInteger(0);

        AtomicInteger thresholdExceededPrimaryWritesCount = new AtomicInteger(0);
        AtomicInteger readSessionNotAvailablePrimaryWritesCount = new AtomicInteger(0);
        AtomicInteger isFailurePrimaryWritesCount = new AtomicInteger(0);
        AtomicInteger thresholdExceededSecondaryWritesCount = new AtomicInteger(0);
        AtomicInteger readSessionNotAvailableSecondaryWritesCount = new AtomicInteger(0);
        AtomicInteger isFailureSecondaryWritesCount = new AtomicInteger(0);
        AtomicInteger thresholdExceededPrimaryReadsCount = new AtomicInteger(0);
        AtomicInteger readSessionNotAvailablePrimaryReadsCount = new AtomicInteger(0);
        AtomicInteger isFailurePrimaryReadsCount = new AtomicInteger(0);

        initializeServiceResources(cfg, loopCount, runId);

        CosmosAsyncClient clientTargetedToFirstPreferredRegion = createClient(
                cfg,
                loopCount,
                runId,
                "onWriteOrReadToFirstPreferredRegion",
                readSessionNotAvailablePrimaryWritesCount,
                thresholdExceededPrimaryWritesCount,
                isFailurePrimaryWritesCount,
                readSessionNotAvailablePrimaryReadsCount,
                thresholdExceededPrimaryReadsCount,
                isFailurePrimaryReadsCount);

        CosmosAsyncClient clientTargetedToOtherPreferredRegions = createClient(
                cfg,
                loopCount,
                runId,
                "onWriteToOtherPreferredRegion",
                readSessionNotAvailableSecondaryWritesCount,
                thresholdExceededSecondaryWritesCount,
                isFailureSecondaryWritesCount,
                null,
                null,
                null);

        executorForWritesAgainstFirstPreferredRegion
                = new ScheduledThreadPoolExecutor(1, new CosmosDaemonThreadFactory("WriteAgainstFirstPreferredRegion"));
        executorForReadsAgainstFirstPreferredRegion
                = new ScheduledThreadPoolExecutor(1, new CosmosDaemonThreadFactory("ReadAgainstFirstPreferredRegion"));

        executorForWritesAgainstOtherPreferredRegions
                = new ScheduledThreadPoolExecutor(satelliteRegionWriterThreadCount, new CosmosDaemonThreadFactory("WriteAgainstOtherPreferredRegions"));

        executorForWritesAgainstFirstPreferredRegion.setRemoveOnCancelPolicy(true);
        executorForWritesAgainstFirstPreferredRegion.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        executorForReadsAgainstFirstPreferredRegion.setRemoveOnCancelPolicy(true);
        executorForReadsAgainstFirstPreferredRegion.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        executorForWritesAgainstOtherPreferredRegions.setRemoveOnCancelPolicy(true);
        executorForWritesAgainstOtherPreferredRegions.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

        scheduledFutureForWriteAgainstFirstPreferredRegion
                = executorForWritesAgainstFirstPreferredRegion.schedule(
                () -> onTryWriteToFirstPreferredRegion(clientTargetedToFirstPreferredRegion, cfg, shouldStop, totalWriteCountFromPrimaryWriter, totalSuccessfulWriteAttemptsFromPrimaryWriter),
                1000,
                TimeUnit.MILLISECONDS
        );

        scheduledFutureForReadAgainstFirstPreferredRegion
                = executorForReadsAgainstFirstPreferredRegion.schedule(
                () -> onTryReadFromFirstPreferredRegion(clientTargetedToFirstPreferredRegion, cfg, shouldStop, totalReadCountFromPrimaryReader, totalSuccessfulReadCountFromPrimaryReader),
                1000,
                TimeUnit.MILLISECONDS
        );

        for (int i = 0; i < satelliteRegionWriterThreadCount; i++) {

            final int finalI = i;

            scheduledFuturesForWriteAgainstOtherPreferredRegions[i]
                    = executorForWritesAgainstOtherPreferredRegions.schedule(
                    () -> onWriteToOtherPreferredRegions(clientTargetedToOtherPreferredRegions, cfg, finalI, shouldStop, totalWriteCountFromSecondaryWriter, totalSuccessfulWriteAttemptsFromSecondaryWriter),
                    1000,
                    TimeUnit.MILLISECONDS
            );
        }

        for (int i = 0; i < loopCount; i++) {

            logger.info("In loop : {}", i);

            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        logger.info("Run ending...");
        shouldStop.set(true);

        if (!scheduledFutureForWriteAgainstFirstPreferredRegion.isDone()) {
            scheduledFutureForWriteAgainstFirstPreferredRegion.cancel(true);
        }

        if (!scheduledFutureForReadAgainstFirstPreferredRegion.isDone()) {
            scheduledFutureForReadAgainstFirstPreferredRegion.cancel(true);
        }

        for (int i = 0; i < satelliteRegionWriterThreadCount; i++) {
            if (!scheduledFuturesForWriteAgainstOtherPreferredRegions[i].isDone()) {
                scheduledFuturesForWriteAgainstOtherPreferredRegions[i].cancel(true);
            }
        }

        executorForReadsAgainstFirstPreferredRegion.shutdown();
        executorForWritesAgainstOtherPreferredRegions.shutdown();
        executorForWritesAgainstOtherPreferredRegions.shutdown();

        printConfiguration(cfg);
        printStatistics(
                globalCache,
                totalSuccessfulWriteAttemptsFromPrimaryWriter,
                totalSuccessfulWriteAttemptsFromSecondaryWriter,
                totalSuccessfulReadCountFromPrimaryReader,
                totalWriteCountFromPrimaryWriter,
                totalWriteCountFromSecondaryWriter,
                totalReadCountFromPrimaryReader,
                readSessionNotAvailablePrimaryWritesCount,
                readSessionNotAvailableSecondaryWritesCount,
                readSessionNotAvailablePrimaryReadsCount,
                thresholdExceededPrimaryWritesCount,
                thresholdExceededSecondaryWritesCount,
                thresholdExceededPrimaryReadsCount,
                isFailurePrimaryWritesCount,
                isFailureSecondaryWritesCount,
                isFailurePrimaryReadsCount);
    }

    private static CosmosAsyncClient createClient(
            Configuration cfg,
            int loopCount,
            String runId,
            String clientId,
            AtomicInteger readSessionNotAvailableCountForWrites,
            AtomicInteger thresholdExceededCountForWrites,
            AtomicInteger failureCountForWrites,
            AtomicInteger readSessionNotAvailableCountForReads,
            AtomicInteger thresholdExceededCountForReads,
            AtomicInteger failureCountForReads) {
        CosmosClientBuilder builder = new CosmosClientBuilder();

        builder.sessionRetryOptions(
                new SessionRetryOptionsBuilder()
                        .regionSwitchHint(cfg.getRegionSwitchHint())
                        .maxRetriesPerRegion(cfg.getMaxRetryCountInRegion())
                        .minTimeoutPerRegion(Duration.ofMillis(cfg.getMinRetryTimeInRegionInMs()))
                        .build());

        builder.consistencyLevel(ConsistencyLevel.SESSION);
        builder.endpoint(cfg.getServiceEndpoint());
        builder.key(cfg.getMasterKey());
        builder.multipleWriteRegionsEnabled(true);
        builder.preferredRegions(Arrays.asList("West US", "South Central US", "East US"));

        if (cfg.getConnectionMode() == ConnectionMode.GATEWAY) {
            builder.gatewayMode();
        } else {
            builder.directMode();
        }

        CosmosDiagnosticsHandler diagnosticsHandler = (diagnosticsContext, traceContext) -> {
            boolean hasReadSessionNotAvailable =  diagnosticsContext
                    .getDistinctCombinedClientSideRequestStatistics()
                    .stream()
                    .anyMatch(
                            clientStats -> clientStats.getResponseStatisticsList().stream().anyMatch(
                                    responseStats -> responseStats.getStoreResult().getStoreResponseDiagnostics().getStatusCode() == 404 &&
                                            responseStats.getStoreResult().getStoreResponseDiagnostics().getSubStatusCode() == 1002
                            )
                    );

            if (diagnosticsContext.isFailure()
                    || diagnosticsContext.isThresholdViolated()
                    || diagnosticsContext.getContactedRegionNames().size() > 1
                    || hasReadSessionNotAvailable) {

                if (diagnosticsContext.getOperationType().equals("Create")) {

                    if (diagnosticsContext.isFailure() && failureCountForWrites != null) {
                        failureCountForWrites.incrementAndGet();
                    }


                    if (diagnosticsContext.isThresholdViolated() && thresholdExceededCountForWrites != null) {
                        thresholdExceededCountForWrites.incrementAndGet();
                    }


                    if (hasReadSessionNotAvailable && readSessionNotAvailableCountForWrites != null) {
                        readSessionNotAvailableCountForWrites.incrementAndGet();
                    }
                } else if (diagnosticsContext.getOperationType().equals("Read")) {
                    if (diagnosticsContext.isFailure() && failureCountForReads != null) {
                        failureCountForReads.incrementAndGet();
                    }


                    if (diagnosticsContext.isThresholdViolated() && thresholdExceededCountForReads != null) {
                        thresholdExceededCountForReads.incrementAndGet();
                    }


                    if (hasReadSessionNotAvailable && readSessionNotAvailableCountForReads != null) {
                        readSessionNotAvailableCountForReads.incrementAndGet();
                    }
                }

                logger.info(
                        "{} IsFailure: {}, IsThresholdViolated: {}, ContactedRegions: {}, hasReadSessionNotAvailable: {}  CTX: {}",
                        clientId,
                        diagnosticsContext.isFailure(),
                        diagnosticsContext.isThresholdViolated(),
                        String.join(", ", diagnosticsContext.getContactedRegionNames()),
                        hasReadSessionNotAvailable,
                        diagnosticsContext.toJson());
            }
        };

        CosmosDiagnosticsThresholds thresholds = new CosmosDiagnosticsThresholds()
                .setNonPointOperationLatencyThreshold(Duration.ofMinutes(60))
                .setPointOperationLatencyThreshold(Duration.ofMillis(90));
        CosmosClientTelemetryConfig diagnosticsConfig = new CosmosClientTelemetryConfig()
                .clientCorrelationId(constructCorrelationId(cfg, runId, clientId, loopCount))
                .diagnosticsThresholds(thresholds)
                .diagnosticsHandler(diagnosticsHandler);
        builder.clientTelemetryConfig(diagnosticsConfig);

        return builder.buildAsyncClient();
    }

    private static int computeLoopCount(Duration runDuration) {
        return (int) runDuration.getSeconds() / 10;
    }

    private static void initializeServiceResources(Configuration cfg, int loopCount, String runId) {

        String databaseName = cfg.getDatabaseName();
        String containerName = cfg.getContainerName();

        client = createClient(cfg, loopCount, runId, "initializeServiceResources", null, null, null, null, null, null);

        client
                .createDatabaseIfNotExists(databaseName)
                .doOnSuccess(response -> logger.info("Database with name : {} exists or created successfully!", databaseName))
                .block();
        client
                .getDatabase(databaseName)
                .createContainerIfNotExists(
                        containerName,
                        "/id",
                        ThroughputProperties.createAutoscaledThroughput(cfg.getContainerInitialThroughput())
                )
                .doOnSuccess(response -> logger.info("Container with name : {} exists or created successfully!", containerName))
                .block();

        container = client
                .getDatabase(cfg.getDatabaseName())
                .getContainer(cfg.getContainerName());

        if (shouldCleanUp.get()) {
            logger.info("Waiting for container {}/{} to be created across all regions...", databaseName, containerName);
            try {
                Thread.sleep(20_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void onTryWriteToFirstPreferredRegion(
            CosmosAsyncClient clientForFirstPreferredRegion,
            Configuration cfg,
            AtomicBoolean shouldStopLoop,
            AtomicInteger totalWriteAttempts,
            AtomicInteger totalSuccessfulWriteAttempts) {

        logger.info("WRITE to first preferred region started...");

        CosmosAsyncContainer container = clientForFirstPreferredRegion.getDatabase(cfg.getDatabaseName()).getContainer(cfg.getContainerName());

        while (!shouldStopLoop.get()) {
            writeLoop(container, shouldStopLoop, Arrays.asList("East US"), 100, true, totalWriteAttempts, totalSuccessfulWriteAttempts);

            logger.info("Simulating fail-over...");
            writeLoop(container, shouldStopLoop, Arrays.asList("West US", "East US"), 100, true, totalWriteAttempts, totalSuccessfulWriteAttempts);

            logger.info("Moving back writes to first preferred region...");
            writeLoop(container, shouldStopLoop, Arrays.asList("East US"), 1000, true, totalWriteAttempts, totalSuccessfulWriteAttempts);
        }
    }

    private static void onWriteToOtherPreferredRegions(
            CosmosAsyncClient clientForOtherPreferredRegions,
            Configuration cfg,
            int taskId,
            AtomicBoolean shouldStopLoop,
            AtomicInteger totalWriteAttempts,
            AtomicInteger totalSuccessfulWriteAttempts) {

        logger.info("WRITE to other preferred region started through task : {}...", taskId);

        CosmosAsyncContainer container = clientForOtherPreferredRegions.getDatabase(cfg.getDatabaseName()).getContainer(cfg.getContainerName());

        while (!shouldStopLoop.get()) {
            writeLoop(container, shouldStopLoop, Arrays.asList("West US", "East US"), 1000, false, totalWriteAttempts, totalSuccessfulWriteAttempts);
        }
    }

    private static void onTryReadFromFirstPreferredRegion(
            CosmosAsyncClient clientForFirstPreferredRegion,
            Configuration cfg,
            AtomicBoolean shouldStopLoop,
            AtomicInteger totalReadAttempts,
            AtomicInteger totalSuccessfulReadAttempts) {
        logger.info("READ attempt from first preferred started...");

        CosmosAsyncContainer container = clientForFirstPreferredRegion.getDatabase(cfg.getDatabaseName()).getContainer(cfg.getContainerName());

        while (!shouldStopLoop.get()) {
            readLoop(container, shouldStopLoop, Arrays.asList(""), 1000, totalReadAttempts, totalSuccessfulReadAttempts);
        }
    }

    private static void writeLoop(
            CosmosAsyncContainer container,
            AtomicBoolean shouldLoopTerminate,
            List<String> excludedRegions,
            Integer maxIterations,
            boolean shouldCache,
            AtomicInteger totalWriteAttempts,
            AtomicInteger totalSuccessfulWriteAttempts) {

        int iterationCount = 0;

        while (!shouldLoopTerminate.get() && iterationCount < maxIterations) {
            iterationCount++;
            write(container, excludedRegions, shouldCache, totalWriteAttempts, totalSuccessfulWriteAttempts);
        }
    }

    private static void readLoop(
            CosmosAsyncContainer container,
            AtomicBoolean shouldLoopTerminate,
            List<String> excludedRegions,
            Integer maxIterations,
            AtomicInteger totalReadAttempts,
            AtomicInteger totalSuccessfulReadAttempts) {

        int iterationCount = 0;

        while (!shouldLoopTerminate.get() && iterationCount < maxIterations) {
            iterationCount++;
            read(container, excludedRegions, totalReadAttempts, totalSuccessfulReadAttempts);
        }
    }

    private static void write(
            CosmosAsyncContainer container,
            List<String> excludedRegions,
            boolean shouldCache,
            AtomicInteger totalWriteAttempts,
            AtomicInteger totalSuccessfulWriteAttempts) {
        String id = UUID.randomUUID().toString();
        CosmosItemRequestOptions options = new CosmosItemRequestOptions().setExcludedRegions(excludedRegions);
        try {
            logger.debug("--> write {}", id);
            container
                    .createItem(getDocumentDefinition(id), new PartitionKey(id), new CosmosItemRequestOptions().setExcludedRegions(excludedRegions))
                    .doOnSubscribe(unused -> totalWriteAttempts.incrementAndGet())
                    .doOnSuccess(response -> {
                        if (shouldCache) {
                            globalCache.offer(id);
                        }
                        totalSuccessfulWriteAttempts.incrementAndGet();
                    })
                    .block();
            logger.debug("<-- write {}", id);
        } catch (CosmosException error) {
            logger.info("COSMOS EXCEPTION - CTX: {}", error.getDiagnostics().getDiagnosticsContext().toJson(), error);
            throw error;
        }
    }

    private static void read(
            CosmosAsyncContainer container,
            List<String> excludedRegions,
            AtomicInteger totalReadAttempts,
            AtomicInteger totalSuccessfulReadCount) {

        if (globalCache.isEmpty()) {
            logger.info("Empty global cache!");
            return;
        }

        String id = globalCache.getLast();

        CosmosItemRequestOptions options = new CosmosItemRequestOptions().setExcludedRegions(excludedRegions);

        try {
            logger.info("Attempting read of id : {}", id);
            container
                    .readItem(id, new PartitionKey(id), options, ObjectNode.class)
                    .doOnSubscribe(unused -> totalReadAttempts.incrementAndGet())
                    .doOnSuccess(unused -> totalSuccessfulReadCount.incrementAndGet())
                    .block();
        } catch (CosmosException error) {
            logger.info("COSMOS EXCEPTION - CTX: {}", error.getDiagnostics().getDiagnosticsContext().toJson(), error);
            throw error;
        }
    }

    private static ObjectNode getDocumentDefinition(String documentId) {
        String json = String.format(
                "{ \"id\": \"%s\", \"mypk\": \"%s\" }",
                documentId,
                documentId);

        try {
            return
                    OBJECT_MAPPER.readValue(json, ObjectNode.class);
        } catch (JsonProcessingException jsonError) {
            fail("No json processing error expected", jsonError);

            throw new IllegalStateException("No json processing error expected", jsonError);
        }
    }

    private static String constructCorrelationId(Configuration cfg, String runId, String clientId, int loopCount) {

        String clientCorrelationId = "runId-"
                + runId
                + "-mirc-"
                + cfg.getMaxRetryCountInRegion()
                + "-mrrt-"
                + cfg.getMinRetryTimeInRegionInMs()
                + "-rsh-"
                + (cfg.getRegionSwitchHint() == CosmosRegionSwitchHint.LOCAL_REGION_PREFERRED ? "lrp" : "rrp")
                + "-cc-"
                + cfg.getSatelliteRegionWriterThreadCount()
                + "-lc-"
                + loopCount
                + "-cid-"
                + clientId;

        return clientCorrelationId;
    }

    private static void printConfiguration(Configuration cfg) {
        logger.info("Configuration : {}", cfg);
    }

    private static void printStatistics(
            List<String> globalCache,
            AtomicInteger totalSuccessfulWriteCountFromPrimaryWriter,
            AtomicInteger totalSuccessfulWriteCountFromSecondaryWriter,
            AtomicInteger totalSuccessfulReadCountFromPrimaryReader,
            AtomicInteger totalWriteCountFromPrimaryWriter,
            AtomicInteger totalWriteCountFromSecondaryWriter,
            AtomicInteger totalReadCountFromPrimaryReader,
            AtomicInteger primaryWriterReadSessionNotAvailableCount,
            AtomicInteger secondaryWriterReadSessionNotAvailableCount,
            AtomicInteger primaryReaderReadSessionNotAvailableCount,
            AtomicInteger primaryWriterThresholdExceededCount,
            AtomicInteger secondaryWriterThresholdExceededCount,
            AtomicInteger primaryReaderThresholdExceededCount,
            AtomicInteger primaryWriterFailureCount,
            AtomicInteger secondaryWriterFailureCount,
            AtomicInteger primaryReaderFailureCount) {

        logger.info("|--------------------------------------------------------|");
        logger.info("Size of global cache (populated by primary writes) : {}", globalCache.size());
        logger.info("|--------------------------------------------------------|");
        logger.info("Total successful write counts from primary writer : {}", totalSuccessfulWriteCountFromPrimaryWriter.get());
        logger.info("Total write counts from primary writer : {}", totalWriteCountFromPrimaryWriter.get());
        logger.info("Total successful write counts from secondary writer : {}", totalSuccessfulWriteCountFromSecondaryWriter.get());
        logger.info("Total write counts from secondary writer : {}", totalWriteCountFromSecondaryWriter.get());
        logger.info("Total successful read counts from primary reader : {}", totalSuccessfulReadCountFromPrimaryReader.get());
        logger.info("Total read counts from primary reader : {}", totalReadCountFromPrimaryReader.get());
        logger.info("|--------------------------------------------------------|");
        logger.info("Primary writes with 404/1002 : {}", primaryWriterReadSessionNotAvailableCount);
        logger.info("Secondary writes with 404/1002 : {}", secondaryWriterReadSessionNotAvailableCount);
        logger.info("Primary reads with 404/1002 : {}", primaryReaderReadSessionNotAvailableCount);
        logger.info("|--------------------------------------------------------|");
        logger.info("Primary writes with threshold exceed : {}", primaryWriterThresholdExceededCount);
        logger.info("Secondary writes with threshold exceeded : {}", secondaryWriterThresholdExceededCount);
        logger.info("Primary reads with threshold exceeded : {}", primaryReaderThresholdExceededCount);
        logger.info("|--------------------------------------------------------|");
        logger.info("Primary writes with failure : {}", primaryWriterFailureCount);
        logger.info("Secondary writes with failure : {}", secondaryWriterFailureCount);
        logger.info("Primary reads with failure : {}", primaryReaderFailureCount);
        logger.info("|--------------------------------------------------------|");
        logger.info("% of primary reads with 404/1002: {}%", ((double) primaryReaderReadSessionNotAvailableCount.get() / (double) totalReadCountFromPrimaryReader.get()) * 100d);
        logger.info("% of primary writes with 404/1002: {}%", ((double) primaryWriterReadSessionNotAvailableCount.get() / (double) totalWriteCountFromPrimaryWriter.get()) * 100d);
        logger.info("% of secondary writes with 404/1002: {}%", ((double) secondaryWriterReadSessionNotAvailableCount.get() / (double) totalWriteCountFromSecondaryWriter.get()) * 100d);
        logger.info("|--------------------------------------------------------|");
    }
}
