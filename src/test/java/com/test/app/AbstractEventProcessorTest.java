package com.test.app;

import com.test.app.dao.EventDao;
import com.test.app.processor.Worker;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.runAsync;

public abstract class AbstractEventProcessorTest {
    private static final int THREAD_COUNT = 5;
    private final EventDao eventDao;

    public AbstractEventProcessorTest (EventDao eventDao){
        this.eventDao = eventDao;
    }

    protected final void executeTest(Stream<String> stream) throws Exception {
        final Map<String, JSONObject> map = new ConcurrentHashMap();
        final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        try {
            CompletableFuture<?>[] futureList = stream.map(line -> runAsync(new Worker(line, map, eventDao), executorService)).toArray(CompletableFuture<?>[]::new);
            CompletableFuture completableFuture = CompletableFuture.allOf(futureList);
            completableFuture.get();
        } finally {
            executorService.shutdown();
        }
    }
}
