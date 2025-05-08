package ru.nms.diplom.searchclient.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static ru.nms.diplom.searchclient.service.DataFrameLoader.loadRelevantPassages;

public class ExportQueriesForBenchmark {

    public static void main(String[] args) throws IOException {
        String inputCsv = "./relevant_passages.csv";     // input CSV file path
        String outputTxt = "./queries.txt";              // output for ghz
        int queriesAmount = 1000;                        // number of queries to export

        Map<String, Integer> queries = loadRelevantPassages(inputCsv, queriesAmount);

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputTxt))) {
            for (String query : queries.keySet()) {
                String jsonLine = String.format(
                        "{\"query\": \"%s\", \"k\": 10, \"shardLimitCoefficient\": 1.0, \"isCoefficientAbsolute\": false}",
                        escapeJson(query));
                writer.write(jsonLine);
                writer.newLine();
            }
        }

        System.out.println("Exported " + queries.size() + " queries to " + outputTxt);
    }

    private static String escapeJson(String input) {
        return input.replace("\"", "\\\"").replace("\\", "\\\\");
    }
}

