package com.benchmark;

import com.azure.cosmos.CosmosAsyncClient;

public abstract class Workload {
    public abstract void execute(Configuration cfg);

    CosmosAsyncClient buildClient(Configuration cfg) {
        return null;
    }
}
