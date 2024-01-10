package com.benchmark;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.CosmosRegionSwitchHint;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import java.time.Duration;
import java.util.Locale;

public class Configuration {

    private static final int DEFAULT_INITIAL_CONTAINER_THROUGHPUT = 10_100;
    private static final int DEFAULT_SATELLITE_REGION_WRITER_COUNT = 5;
    private static final ConnectionMode DEFAULT_CONNECTION_MODE = ConnectionMode.DIRECT;
    private static final String DEFAULT_DATABASE_NAME = "session-consistency-abhm-tests-db";
    private static final String DEFAULT_CONTAINER_NAME = "session-consistency-abhm-tests-db";
    private static final int DEFAULT_MAX_RETRY_COUNT_IN_REGION = 2;
    private static final int DEFAULT_MIN_RETRY_TIME_IN_REGION_IN_MS = 1_000;
    private static final Duration DEFAULT_RUN_DURATION = Duration.ofSeconds(60);
    private static final boolean DEFAULT_SHOULD_FAILOVER_WRITES_TO_SATELLITE_REGIONS = true;

    @Parameter(names = "-serviceEndpoint", description = "The service endpoint.")
    private String serviceEndpoint;

    @Parameter(names = "-masterKey", description = "The master key for the account.")
    private String masterKey;

    @Parameter(names = "-containerInitialThroughput", description = "The initial provisioned throughput for container.")
    private int containerInitialThroughput;

    @Parameter(names = "-databaseName", description = "The database name.")
    private String databaseName;

    @Parameter(names = "-containerName", description = "The container name.")
    private String containerName;

    @Parameter(names = "-connectionMode", description = "The connectivity mode - DIRECT / GATEWAY.", converter = ConnectionModeConverter.class)
    private ConnectionMode connectionMode;

    @Parameter(names = "-shouldFailoverWritesToSatelliteRegions", description = "A boolean parameter to indicate whether writes should failover to satellite regions.", arity = 1)
    private boolean shouldFailoverWritesToSatelliteRegions;

    @Parameter(names = "-satelliteRegionWriterThreadCount", description = "The no. of writer threads which write to satellite regions.")
    private int satelliteRegionWriterThreadCount;

    @Parameter(names = "-minRetryTimeInRegionInMs", description = "The minimum time to retry in a given region for 404/1002.")
    private int minRetryTimeInRegionInMs;

    @Parameter(names = "-maxRetryCountInRegion", description = "The maximum no. of times to retry in a given region for 404/1002.")
    private int maxRetryCountInRegion;

    @Parameter(names = "-regionSwitchHint", description = "The region switch hint - LOCAL_REGION_PREFERRED / REMOTE_REGION_PREFERRED to be used for 404/1002 retries.", converter = RegionSwitchHintConverter.class)
    private CosmosRegionSwitchHint regionSwitchHint;

    @Parameter(names = "-runDuration", description = "The duration for which the workload should run.", converter = DurationConverter.class)
    private Duration runDuration;

    @Parameter(names = "-readAfterWriteType", description = "The type of read which determines what written document to be read.", converter = ReadAfterWriteTypeConverter.class)
    private ReadAfterWriteType readAfterWriteType;

    public String getServiceEndpoint() {
        return serviceEndpoint;
    }

    public String getMasterKey() {
        return masterKey;
    }

    public int getContainerInitialThroughput() {
        return containerInitialThroughput;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getContainerName() {
        return containerName;
    }

    public ConnectionMode getConnectionMode() {
        return connectionMode;
    }

    public boolean isShouldFailoverWritesToSatelliteRegions() {
        return shouldFailoverWritesToSatelliteRegions;
    }

    public int getSatelliteRegionWriterThreadCount() {
        return satelliteRegionWriterThreadCount;
    }

    public int getMinRetryTimeInRegionInMs() {
        return minRetryTimeInRegionInMs;
    }

    public CosmosRegionSwitchHint getRegionSwitchHint() {
        return regionSwitchHint;
    }

    public Duration getRunDuration() {
        return runDuration;
    }

    public int getMaxRetryCountInRegion() {
        return maxRetryCountInRegion;
    }

    public ReadAfterWriteType getReadAfterWriteType() {
        return readAfterWriteType;
    }

    public void populateWithDefaults() {
        this.containerInitialThroughput = DEFAULT_INITIAL_CONTAINER_THROUGHPUT;
        this.connectionMode = DEFAULT_CONNECTION_MODE;
        this.databaseName = DEFAULT_DATABASE_NAME;
        this.containerName = DEFAULT_CONTAINER_NAME;
        this.satelliteRegionWriterThreadCount = DEFAULT_SATELLITE_REGION_WRITER_COUNT;
        this.maxRetryCountInRegion = DEFAULT_MAX_RETRY_COUNT_IN_REGION;
        this.minRetryTimeInRegionInMs = DEFAULT_MIN_RETRY_TIME_IN_REGION_IN_MS;
        this.shouldFailoverWritesToSatelliteRegions = DEFAULT_SHOULD_FAILOVER_WRITES_TO_SATELLITE_REGIONS;
        this.runDuration = DEFAULT_RUN_DURATION;
    }

    static class DurationConverter implements IStringConverter<Duration> {
        @Override
        public Duration convert(String value) {
            if (value == null) {
                return null;
            }

            return Duration.parse(value);
        }
    }

    static class RegionSwitchHintConverter implements IStringConverter<CosmosRegionSwitchHint> {

        @Override
        public CosmosRegionSwitchHint convert(String value) {
            String normalizedRegionSwitchHintAsString
                    = value.toLowerCase(Locale.ROOT).replace(" ", "").trim();

            CosmosRegionSwitchHint result;

            if (normalizedRegionSwitchHintAsString.equals("local_region_preferred")) {
                result = CosmosRegionSwitchHint.LOCAL_REGION_PREFERRED;
            } else {
                result = CosmosRegionSwitchHint.REMOTE_REGION_PREFERRED;
            }

            return result;
        }
    }

    static class ConnectionModeConverter implements IStringConverter<ConnectionMode> {

        @Override
        public ConnectionMode convert(String value) {
            String normalizedConnectionModeAsString
                    = value.toLowerCase(Locale.ROOT).replace(" ", "").trim();

            ConnectionMode result;

            if (normalizedConnectionModeAsString.equals("gateway")) {
                result = ConnectionMode.GATEWAY;
            } else {
                result = ConnectionMode.DIRECT;
            }

            return result;
        }
    }

    static class ReadAfterWriteTypeConverter implements IStringConverter<ReadAfterWriteType> {

        @Override
        public ReadAfterWriteType convert(String value) {

            String normalizedReadAfterWriteTypeAsString = value.toLowerCase(Locale.ROOT).replace(" ", "").trim();

            if (normalizedReadAfterWriteTypeAsString.equals("readrandomwrite")) {
                return ReadAfterWriteType.READ_RANDOM_WRITE;
            } else if (normalizedReadAfterWriteTypeAsString.equals("readnewerwrite")) {
                return ReadAfterWriteType.READ_NEWER_WRITE;
            } else {
                return ReadAfterWriteType.READ_RANDOM_WRITE;
            }
        }
    }

    @Override
    public String toString() {
        return "Configuration{" +
                "serviceEndpoint='" + serviceEndpoint + '\'' +
                ", containerInitialThroughput=" + containerInitialThroughput +
                ", databaseName='" + databaseName + '\'' +
                ", containerName='" + containerName + '\'' +
                ", connectionMode=" + (connectionMode == ConnectionMode.DIRECT ? "direct" : "gateway") +
                ", shouldFailoverWritesToSatelliteRegions=" + shouldFailoverWritesToSatelliteRegions +
                ", satelliteRegionWriterThreadCount=" + satelliteRegionWriterThreadCount +
                ", minRetryTimeInRegionInMs=" + minRetryTimeInRegionInMs +
                ", maxRetryCountInRegion=" + maxRetryCountInRegion +
                ", regionSwitchHint=" + (regionSwitchHint == CosmosRegionSwitchHint.LOCAL_REGION_PREFERRED ? "local_region_preferred" : "remote_region_preferred") +
                ", runDuration=" + runDuration +
                ", readAfterWriteType=" + ((readAfterWriteType == ReadAfterWriteType.READ_NEWER_WRITE) ? "READ_NEWER_WRITE" : "READ_RANDOM_WRITE") +
                '}';
    }
}
