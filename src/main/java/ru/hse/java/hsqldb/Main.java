package ru.hse.java.hsqldb;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        String csvFilePath, tableName;
        if (args.length == 2) {
            csvFilePath = args[0];
            tableName = args[1];
        } else if (args.length == 1) {
            csvFilePath = args[0];
            String path = Paths.get(csvFilePath).getFileName().toString();
            tableName = path.substring(0, path.lastIndexOf('.'));
        } else {
            System.out.println("Wrong number of arguments. Correct format: <path_to_file> [table_name]");
            return;
        }

        CSVParser csvParser = new CSVParser(csvFilePath);
        ArrayList<List<String>> records = csvParser.getRecords();

        try {
            DBConnector dbConnector = new DBConnector();
            dbConnector.addRecordsToDB(tableName, records);
        } catch (Exception e) {
            System.out.println("Failed to connect to DB");
        }
    }
}
