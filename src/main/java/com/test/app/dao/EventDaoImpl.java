package com.test.app.dao;

import com.test.app.constants.Constants;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class EventDaoImpl implements EventDao {

    private static final EventDaoImpl eventDaoImpl = new EventDaoImpl();

    private EventDaoImpl() {
    }

    public static EventDaoImpl getInstance() {
        return eventDaoImpl;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(EventDaoImpl.class);

    private static final String DB_STRING_FILE_EXISTS = "jdbc:hsqldb:file:testdb;ifexists=true";

    private static final String INSERT_QUERY = "insert into event_data_alert (event_id, event_duration, type, host_name, alert) values(?,?,?,?,?)";

    private static final String TRUNCATE_TABLE_QUERY = "truncate table event_data_alert;";
    private static final String RECORD_COUNT_QUERY = "select count(1) from event_data_alert where alert=?;";

    @Override
    public void saveRecord(JSONObject record, long duration, boolean alert) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = DriverManager.getConnection(DB_STRING_FILE_EXISTS);
            statement = connection.prepareStatement(INSERT_QUERY);
            statement.setString(1, record.getString(Constants.ID));
            statement.setLong(2, duration);
            statement.setString(3, record.optString(Constants.TYPE));
            statement.setString(4, record.optString(Constants.HOST_NAME));
            statement.setBoolean(5, alert);
            statement.execute();
            statement.close();
        } catch (SQLException e) {
            LOGGER.error("Error saving record {}", record.toString(), e);
        } finally {
            close(connection, statement);
        }
    }

    @Override
    public void truncateTable() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(DB_STRING_FILE_EXISTS);
            statement = connection.createStatement();
            statement.execute(TRUNCATE_TABLE_QUERY);
        } catch (SQLException e) {
            LOGGER.error("Error truncating table..", e);
        } finally {
            close(connection, statement);
        }
    }

    @Override
    public Long getRecordCount(boolean alert) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        Long count = null;
        try {
            connection = DriverManager.getConnection(DB_STRING_FILE_EXISTS);
            statement = connection.prepareStatement(RECORD_COUNT_QUERY);
            statement.setBoolean(1, alert);
            ResultSet resultSet = statement.executeQuery();
            resultSet.next();
            count = resultSet.getLong(1);
        } catch (SQLException e) {
            LOGGER.error("Error reading record", e);
        } finally {
            close(connection, statement);
        }
        return count;
    }

    private void close(Connection connection, Statement statement) throws SQLException {
        if (null != statement) {
            statement.close();
        }
        if (null != connection) {
            connection.close();
        }
    }

}

