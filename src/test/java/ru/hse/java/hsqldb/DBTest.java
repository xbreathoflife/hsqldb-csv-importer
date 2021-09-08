package ru.hse.java.hsqldb;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DBTest {
    private final List<String> tablesToDrop = List.of("username");

    @BeforeEach
    void dropTables() throws Exception {
        DBConnector dbConnector = new DBConnector();
        for (String table : tablesToDrop) {
            dbConnector.dropTable(table);
        }
        dbConnector.shutdownServer();
    }

    @Test
    void testDB() throws Exception {
        String filePath = "db-data/username.csv";
        String tableName = "username";
        String[] args = {filePath, tableName};
        Main.main(args);

        CSVParser csvParser = new CSVParser("db-data/username.csv");
        ArrayList<List<String>> records = csvParser.getRecords();

        DBConnector dbConnector = new DBConnector();
        ArrayList<List<String>> actualRecords = dbConnector.getRecordsFromDB(tableName, records.get(0));

        Assertions.assertEquals(records, actualRecords);
    }

}