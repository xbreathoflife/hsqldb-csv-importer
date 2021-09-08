package ru.hse.java.hsqldb;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.Getter;

@Getter
public class CSVParser {

    ArrayList<List<String>> records = new ArrayList<>();

    public CSVParser(String filePath) {
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            String[] values;
            while ((values = csvReader.readNext()) != null) {
                records.add(Arrays.asList(values));
            }
        } catch (IOException | CsvValidationException e) {
            System.out.println("Failed to read csv file");
            e.printStackTrace();
        }
    }
}
