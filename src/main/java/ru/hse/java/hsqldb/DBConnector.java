package ru.hse.java.hsqldb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;

public class DBConnector {

    private static final String CONNECTION_STRING = "jdbc:hsqldb:hsql://localhost/testdb";
    private static final String USER = "SA";
    private static final String PASSWORD = "";
    private final Server sonicServer = new Server();
    final String dbLocation = "/Users/aleksa30/Downloads/hsqldb-2.6.0/hsqldb/hsqldb/testdb";

    public DBConnector() throws Exception {
        HsqlProperties props = new HsqlProperties();
        props.setProperty("server.database.0", "file:" + dbLocation + ";");
        props.setProperty("server.dbname.0", "testdb");
        try {
            sonicServer.setProperties(props);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        sonicServer.start();

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (ClassNotFoundException e) {
            System.out.println("Failed to find JDBCDriver");
            e.printStackTrace();
            throw e;
        }
    }

    // TODO: remove spaces
    public void addRecordsToDB(String tableName, ArrayList<List<String>> records) {
        if (records.isEmpty()) {
            System.out.println("Csv file is empty");
            return;
        }

        // Create table (if not exists) using first row of csv file
        List<String> columnNames = records.get(0);
        StringBuilder createTableTemplate = new StringBuilder();
        createTableTemplate.append("CREATE TABLE IF NOT EXISTS ")
                .append(tableName)
                .append(" (");

        String delimiter = "";
        for (String name : columnNames) {
            createTableTemplate.append(delimiter)
                    .append('\"')
                    .append(name)
                    .append('\"')
                    .append(" VARCHAR(255)");
            delimiter = ", ";
        }
        createTableTemplate.append(")");

        try (Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableTemplate.toString());

            // Insert csv file data to created table
            String insertIntoTableTemplate = "INSERT INTO " + tableName + " VALUES (";
            for (int i = 1; i < records.size(); i++) {
                List<String> values = records.get(i);
                StringBuilder sql = new StringBuilder();
                delimiter = "";
                for (String value : values) {
                    sql.append(delimiter)
                            .append('\'')
                            .append(value.replace("'", "''"))
                            .append('\'');
                    delimiter = ", ";
                }
                sql.append(")");
                statement.addBatch(insertIntoTableTemplate + sql);
            }
            statement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            shutdownServer();
        }
    }

    ArrayList<List<String>> getRecordsFromDB(String tableName, List<String> columnNames) {
        String selectQuery = "SELECT * FROM " + tableName;
        try (Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
             PreparedStatement statement = connection.prepareStatement(selectQuery)) {
            statement.clearParameters();
            ArrayList<List<String>> records = new ArrayList<>();
            records.add(columnNames);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (String name : columnNames) {
                    row.add(rs.getString(name));
                }
                records.add(row);
            }

            return records;
        } catch (SQLException e) {
            System.out.println("Failed to get records from DB");
            e.printStackTrace();
            return null;
        } finally {
            shutdownServer();
        }
    }

    public void dropTable(String tableName) {
        String dropQuery = "DROP TABLE " + tableName + " IF EXISTS";

        try (Connection connection = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(dropQuery);
        } catch (SQLException e) {
            System.out.println("Failed to drop table " + tableName);
            e.printStackTrace();
        }
    }

    public void shutdownServer() {
        sonicServer.shutdown();
    }

}
