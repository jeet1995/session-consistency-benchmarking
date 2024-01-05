package com.benchmark;

import com.azure.cosmos.implementation.CosmosDaemonThreadFactory;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final ScheduledThreadPoolExecutor workloadExecutor = new ScheduledThreadPoolExecutor(1, new CosmosDaemonThreadFactory("ExecuteWorkload"));

    public static void main(String[] args) {
        Configuration cfg = new Configuration();
        cfg.populateWithDefaults();

        logger.info("Parsing command-line args...");

        JCommander jCommander = new JCommander(cfg, null, args);
        ReadMyWriteWithWritesToDifferentRegions readMyWriteWithWritesToDifferentRegions
                = new ReadMyWriteWithWritesToDifferentRegions();

        ScheduledFuture<?> task
                = workloadExecutor.schedule(() -> readMyWriteWithWritesToDifferentRegions.execute(cfg), 1000, TimeUnit.MILLISECONDS);

        while (true) {
            if (task.isDone()) {
                workloadExecutor.shutdown();
                break;
            }
        }
    }
}