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
import com.azure.cosmos.implementation.apachecommons.lang.RandomUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.fail;

public class ReadMyWriteWithWritesToDifferentRegions extends Workload {

    private static final Logger logger = LoggerFactory.getLogger(ReadMyWriteWithWritesToDifferentRegions.class);
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
    private static final ConcurrentHashMap<String, String> globalCache = new ConcurrentHashMap<>();
    @Override
    public void execute(Configuration cfg) {

        String runId = UUID.randomUUID().toString();
        logger.info("Starting run with runId : {}", runId);

        int loopCount = computeLoopCount(cfg.getRunDuration());
        logger.info("{} loops worth 10s to run...", loopCount);

        int satelliteRegionWriterThreadCount = Math.min(cfg.getSatelliteRegionWriterThreadCount(), 20);
        AtomicBoolean shouldStop = new AtomicBoolean(false);

        initializeServiceResources(cfg, loopCount, runId);

        CosmosAsyncClient clientTargetedToFirstPreferredRegion = createClient(cfg, loopCount, runId, "onWriteOrReadToFirstPreferredRegion");
        CosmosAsyncClient clientTargetedToOtherPreferredRegions = createClient(cfg, loopCount, runId, "onWriteToOtherPreferredRegion");

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
                () -> onTryWriteToFirstPreferredRegion(clientTargetedToFirstPreferredRegion, cfg, shouldStop),
                1000,
                TimeUnit.MILLISECONDS
        );

        scheduledFutureForReadAgainstFirstPreferredRegion
                = executorForReadsAgainstFirstPreferredRegion.schedule(
                () -> onTryReadFromFirstPreferredRegion(clientTargetedToFirstPreferredRegion, cfg, shouldStop),
                1000,
                TimeUnit.MILLISECONDS
        );

        for (int i = 0; i < satelliteRegionWriterThreadCount; i++) {

            final int finalI = i;

            scheduledFuturesForWriteAgainstOtherPreferredRegions[i]
                    = executorForWritesAgainstOtherPreferredRegions.schedule(
                    () -> onWriteToOtherPreferredRegions(clientTargetedToOtherPreferredRegions, cfg, finalI, shouldStop),
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
        printStatistics(globalCache);
    }

    private static CosmosAsyncClient createClient(Configuration cfg, int loopCount, String runId, String clientId) {
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
        builder.preferredRegions(Arrays.asList("East US", "South Central US", "West US"));

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

        client = createClient(cfg, loopCount, runId, "initializeServiceResources");

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

    private static void onTryWriteToFirstPreferredRegion(CosmosAsyncClient clientForFirstPreferredRegion, Configuration cfg, AtomicBoolean shouldStopLoop) {
        logger.info("WRITE to first preferred region started...");

        List<String> excludedRegions = Arrays.asList("");

        CosmosAsyncContainer container = clientForFirstPreferredRegion.getDatabase(cfg.getDatabaseName()).getContainer(cfg.getContainerName());

        while (!shouldStopLoop.get()) {
            writeLoop(container, shouldStopLoop, excludedRegions, 1000, true);

            logger.info("Simulating fail-over...");
            writeLoop(container, shouldStopLoop, Arrays.asList("East US"), 1000, true);

            logger.info("Moving back writes to first preferred region...");
            writeLoop(container, shouldStopLoop, excludedRegions, 1000, true);
        }
    }

    private static void onWriteToOtherPreferredRegions(CosmosAsyncClient clientForOtherPreferredRegions, Configuration cfg, int taskId, AtomicBoolean shouldStopLoop) {
        logger.info("WRITE to other preferred region started through task : {}...", taskId);

        List<String> excludedRegions = Arrays.asList("East US");

        CosmosAsyncContainer container = clientForOtherPreferredRegions.getDatabase(cfg.getDatabaseName()).getContainer(cfg.getContainerName());

        while (!shouldStopLoop.get()) {
            writeLoop(container, shouldStopLoop, excludedRegions, 1000, false);
        }
    }

    private static void onTryReadFromFirstPreferredRegion(CosmosAsyncClient clientForFirstPreferredRegion, Configuration cfg, AtomicBoolean shouldStopLoop) {
        logger.info("READ attempt from first preferred started...");

        List<String> excludedRegions = Arrays.asList("");

        CosmosAsyncContainer container = clientForFirstPreferredRegion.getDatabase(cfg.getDatabaseName()).getContainer(cfg.getContainerName());

        while (!shouldStopLoop.get()) {
            readLoop(container, shouldStopLoop, excludedRegions, 1000);
        }
    }

    private static void writeLoop(
            CosmosAsyncContainer container,
            AtomicBoolean shouldLoopTerminate,
            List<String> excludedRegions,
            Integer maxIterations,
            boolean shouldCache) {

        int iterationCount = 0;

        while (!shouldLoopTerminate.get() && iterationCount < maxIterations) {
            iterationCount++;
            write(container, excludedRegions, shouldCache);
        }
    }

    private static void readLoop(
            CosmosAsyncContainer container,
            AtomicBoolean shouldLoopTerminate,
            List<String> excludedRegions,
            Integer maxIterations) {

        int iterationCount = 0;

        while (!shouldLoopTerminate.get() && iterationCount < maxIterations) {
            iterationCount++;
            read(container, excludedRegions);
        }
    }

    private static void write(CosmosAsyncContainer container, List<String> excludedRegions, boolean shouldCache) {
        String id = UUID.randomUUID().toString();
        CosmosItemRequestOptions options = new CosmosItemRequestOptions().setExcludedRegions(excludedRegions);
        try {
            logger.debug("--> write {}", id);
            container
                    .createItem(getDocumentDefinition(id), new PartitionKey(id), new CosmosItemRequestOptions().setExcludedRegions(excludedRegions))
                    .doOnSuccess(response -> {
                        if (shouldCache) {
                            globalCache.put(id, id);
                        }
                    })
                    .block();
            logger.debug("<-- write {}", id);
        } catch (CosmosException error) {
            logger.info("COSMOS EXCEPTION - CTX: {}", error.getDiagnostics().getDiagnosticsContext().toJson(), error);
            throw error;
        }
    }

    private static void read(CosmosAsyncContainer container, List<String> excludedRegions) {
        List<String> ids = new ArrayList<>(globalCache.values());

        if (ids.size() == 0) {
            logger.info("Empty global cache!");
            return;
        }

        String id = ids.get(RandomUtils.nextInt(0, ids.size()));
        CosmosItemRequestOptions options = new CosmosItemRequestOptions().setExcludedRegions(excludedRegions);
        try {
            logger.info("Attempting read of id : {}", id);
            container
                    .readItem(id, new PartitionKey(id), options, ObjectNode.class)
                    .doOnSuccess(unused -> logger.info("Successful read of id : {}", id))
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

    private static void printStatistics(Map<String, String> globalCache) {
        logger.info("Count of items written : {}", globalCache.size());
    }
}
