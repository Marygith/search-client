package ru.nms.diplom.searchclient.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class DataFrameLoader {

    public static Map<String, Integer> loadRelevantPassages(String filePath, int queriesAmount){
        Map<String, Integer> relevantPassages = new HashMap<>();

        int counter = 0;
        try (FileReader reader = new FileReader(filePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader().withDelimiter('^'))) {
            for (CSVRecord csvRecord : csvParser) {
                String key = csvRecord.get("query");
                Integer value = Integer.parseInt(csvRecord.get("relevant_passage_id"));
                relevantPassages.put(key, value);
                if (counter++ > queriesAmount) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return relevantPassages;
    }
}
