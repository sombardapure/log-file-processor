package com.test.app.dao;

import org.json.JSONObject;

import java.sql.SQLException;

public interface EventDao {
    public void saveRecord(JSONObject record, long duration, boolean alert) throws SQLException;

    public Long getRecordCount(boolean alert) throws SQLException;

    public void truncateTable() throws SQLException;
}
