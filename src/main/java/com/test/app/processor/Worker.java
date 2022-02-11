package com.test.app.processor;

import com.test.app.dao.EventDao;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

import static com.test.app.constants.Constants.EXECUTION_THRESHOLD;
import static com.test.app.constants.Constants.FINISHED;
import static com.test.app.constants.Constants.ID;
import static com.test.app.constants.Constants.STATE;
import static com.test.app.constants.Constants.TIMESTAMP;

public class Worker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    private String line;
    private Map<String, JSONObject> map;
    private EventDao eventDao;

    private Worker() {
    }

    public Worker(String line, Map<String, JSONObject> map, EventDao eventDao) {
        this.line = line;
        this.map = map;
        this.eventDao = eventDao;
    }

    @Override
    public void run() {
        try {
            processEvent();
        } catch (Exception e) {
            LOGGER.error("Error processing event {}", line, e);
        }

    }

    private void processEvent() throws SQLException {
        JSONObject processingEvent = new JSONObject(line);
        JSONObject event = map.putIfAbsent(processingEvent.getString(ID), processingEvent);
        if (null != event) {
            long executionTime;
            if (FINISHED.equalsIgnoreCase(processingEvent.getString(STATE))) {
                executionTime = processingEvent.getLong(TIMESTAMP) - event.getLong(TIMESTAMP);
            } else {
                executionTime = event.getLong(TIMESTAMP) - processingEvent.getLong(TIMESTAMP);
            }
            eventDao.saveRecord(event, executionTime, executionTime > EXECUTION_THRESHOLD);
            map.remove(event.getString(ID));
        }
    }
}
