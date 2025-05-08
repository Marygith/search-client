package ru.nms.diplom.searchclient;


import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ru.nms.diplom.searchclient.service.MrrService;
import ru.nms.diplom.searchgateway.service.Document;
import ru.nms.diplom.searchgateway.service.SearchGatewayServiceGrpc;
import ru.nms.diplom.searchgateway.service.SearchRequest;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ru.nms.diplom.shardsearch.ShardSearchServiceGrpc;

import static ru.nms.diplom.searchclient.service.DataFrameLoader.loadRelevantPassages;

public class SearchClient {
    public static void main(String[] args) {
        var mrrService = new MrrService();
        var stub = getSearchGatewayStub();
        executeTestCase(3000, 3000, 1.5f, mrrService, stub);
//        executeTestCase(1000, 3000, 1.25f, mrrService, stub);
//        executeTestCase(1000, 3000, 1.0f, mrrService, stub);
//        executeTestCase(1000, 1000, 1.5f, mrrService, stub);
//        executeTestCase(1000, 1000, 1.25f, mrrService, stub);
//        executeTestCase(1000, 1000, 1.0f, mrrService, stub);
        gatherMetrics(6);
//        executeTestCase(3000, 700, 1.5f, mrrService, stub);
//        executeTestCase(1000, 500, 1.5f, mrrService, stub);
//        executeTestCase(10000, 1000, 1.7f, mrrService, stub);
//        executeTestCase(10000, 1000, 2f, mrrService, stub);
//        executeTestCase(10000, 1000, 0f, mrrService, stub);

//        executeTestCase(3000, 3000, 2f, mrrService, stub);
//        executeTestCase(3000, 1000, 2f, mrrService, stub);
//        executeTestCase(3000, 700, 2f, mrrService, stub);
//        executeTestCase(3000, 500, 2f, mrrService, stub);

//        executeTestCase(3000, 3000, 0f, mrrService, stub);
//        executeTestCase(3000, 1000, 0f, mrrService, stub);
//        executeTestCase(3000, 700, 0f, mrrService, stub);
//        executeTestCase(3000, 500, 0f, mrrService, stub);


//        executeTestCase(5000, 3000, 1.5f, mrrService, stub);
//        executeTestCase(5000, 1000, 1.5f, mrrService, stub);
//        executeTestCase(5000, 700, 1.5f, mrrService, stub);
//        executeTestCase(5000, 500, 1.5f, mrrService, stub);

//        executeTestCase(5000, 3000, 2f, mrrService, stub);
//        executeTestCase(5000, 1000, 2f, mrrService, stub);
//        executeTestCase(5000, 700, 2f, mrrService, stub);
//        executeTestCase(5000, 500, 2f, mrrService, stub);

//        executeTestCase(5000, 3000, 0f, mrrService, stub);
//        executeTestCase(5000, 1000, 0f, mrrService, stub);
//        executeTestCase(5000, 700, 0f, mrrService, stub);
//        executeTestCase(5000, 500, 0f, mrrService, stub);

    }

    private static void executeTestCase(int queriesAmount, int k, float coefficient, MrrService mrrService, SearchGatewayServiceGrpc.SearchGatewayServiceBlockingStub stub) {


        Map<String, Integer> relevantPassages = loadRelevantPassages("./relevant_passages.csv", queriesAmount);
        var result = new HashMap<Integer, List<Document>>();
        float resultCounter = 0;
        boolean came25 = false;
        boolean came5 = false;
        boolean came50 = false;
        boolean came75 = false;
        boolean came90 = false;
        double averageDuplicatesPercent = 0;
        double averageResultCount = 0;
        double averageAmountOfReceivedDocs = 0;
        int adjustedk = k;
        long averageDuration = 0;
        for (var entry : relevantPassages.entrySet()) {
            var starttime = System.currentTimeMillis();
            var response = stub.search(SearchRequest.newBuilder()
                    .setK(k)
                    .setShardLimitCoefficient(coefficient)
                    .setIsCoefficientAbsolute(false)
                    .setQuery(entry.getKey())
                    .build());
            averageDuration += System.currentTimeMillis() - starttime;
            averageResultCount += response.getResultsCount();
            averageAmountOfReceivedDocs += response.getMeta().getAmountOfReceivedDocs();
            averageDuplicatesPercent += (double) (response.getMeta().getAmountOfReceivedDocs() - response.getResultsCount()) / response.getMeta().getAmountOfReceivedDocs();
            adjustedk = response.getMeta().getAdjustedK();

            if (response.getResultsCount() == 0) break;
            result.put(entry.getValue(), mrrService.normalizeScores(response.getResultsList()));
            resultCounter++;
            if (resultCounter / queriesAmount >= 0.90 && !came90) {
                System.out.println("90% of test is ready, timestamp: " + Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
                came90 = true;
            } else if (resultCounter / queriesAmount >= 0.75 && !came75) {
                System.out.println("75% of test is ready, timestamp: " + Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
                came75 = true;
            } else if (resultCounter / queriesAmount >= 0.5 && !came50) {
                System.out.println("50% of test is ready, timestamp: " + Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
                came50 = true;
            } else if (resultCounter / queriesAmount >= 0.25 && !came25) {
                System.out.println("25% of test is ready, timestamp: " + Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
                came25 = true;
            } else if (resultCounter / queriesAmount >= 0.05 && !came5) {
                System.out.println("5% of test is ready, timestamp: " + Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault()));
                came5 = true;
            }
        }
        System.out.println(
                "\n\n\nMRR with adjusted coeffs: " + mrrService.computeMRR(result, 5.4246, 0.7427)
                        + ", \nMRR with only vector: " + mrrService.computeMRR(result, 1, 0)
                        + ", \nqueries amount: " + queriesAmount
                        + ", \noriginal K: " + k
                        + ", \nadjusted K: " + adjustedk
                        + ", \naverage percent of duplicates: " + averageDuplicatesPercent / queriesAmount
                        + ", \naverage result count: " + averageResultCount / queriesAmount
                        + ", \naverage received docs count: " + averageAmountOfReceivedDocs / queriesAmount
                        + ", \naverage duration: " + (double )averageDuration / queriesAmount / 1000
                        + ", \ncoefficient: " + coefficient
        );
    }

    private static SearchGatewayServiceGrpc.SearchGatewayServiceBlockingStub getSearchGatewayStub() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("stgoxygensearch32445z503.h.o3.ru", 8080)
//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();
        return SearchGatewayServiceGrpc.newBlockingStub(channel);
    }

    private static void gatherMetrics(int nodesAmount) {
        for (int i = 1; i <= nodesAmount; i++) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress("stgoxygensearch32445z503.h.o3.ru", 9090 + i)
//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8080)
                .usePlaintext()
                .build();

            var stub = ShardSearchServiceGrpc.newBlockingStub(channel);

            var metricsData = stub.getMetricData(Empty.newBuilder().build());

            double averageFaissSearchDocTime = 0;
            double averageFaissSimilarityScoresTime = 0;
            int faissSearchDocAmount = 0;
            int faissSimilarityScoresAmount = 0;

            double averageLuceneSearchDocTime = 0;
            double averageLuceneSimilarityScoresTime = 0;
            int luceneSearchDocAmount = 0;
            int luceneSimilarityScoresAmount = 0;

            double averageFaissProxySimilarityScoresTime = 0;
            int faissProxySimilarityScoresAmount = 0;

            double averageLuceneProxySimilarityScoresTime = 0;
            int luceneProxySimilarityScoresAmount = 0;
            for (var faissShardMetrics: metricsData.getFaissShardMetricsList()) {

                var averageSearchDocTime = (double) faissShardMetrics.getSearchDocsTime() / faissShardMetrics.getAmountOfSearchDocsRequests();
                var averageSimilarityScoresTime = (double) faissShardMetrics.getSimilarityDocsTime() / faissShardMetrics.getAmountOfSimilarityDocsRequests();

                averageFaissSearchDocTime += averageSearchDocTime;
                averageFaissSimilarityScoresTime += averageSimilarityScoresTime;
                faissSearchDocAmount += faissShardMetrics.getAmountOfSearchDocsRequests();
                faissSimilarityScoresAmount += faissShardMetrics.getAmountOfSimilarityDocsRequests();
            }

            for (var faissShardMetrics: metricsData.getFaissProxyShardMetricsList()) {

                var averageSimilarityScoresTime = (double) faissShardMetrics.getSimilarityDocsTime() / faissShardMetrics.getAmountOfSimilarityDocsRequests();

                averageFaissProxySimilarityScoresTime += averageSimilarityScoresTime;
                faissProxySimilarityScoresAmount += faissShardMetrics.getAmountOfSimilarityDocsRequests();
            }

            for (var luceneShardMetrics: metricsData.getLuceneShardMetricsList()) {

                var averageSearchDocTime = (double) luceneShardMetrics.getSearchDocsTime() / luceneShardMetrics.getAmountOfSearchDocsRequests();
                var averageSimilarityScoresTime = (double) luceneShardMetrics.getSimilarityDocsTime() / luceneShardMetrics.getAmountOfSimilarityDocsRequests();

                averageLuceneSearchDocTime += averageSearchDocTime;
                averageLuceneSimilarityScoresTime += averageSimilarityScoresTime;
                luceneSearchDocAmount += luceneShardMetrics.getAmountOfSearchDocsRequests();
                luceneSimilarityScoresAmount += luceneShardMetrics.getAmountOfSimilarityDocsRequests();
            }

            for (var luceneShardMetrics: metricsData.getLuceneProxyShardMetricsList()) {

                var averageSimilarityScoresTime = (double) luceneShardMetrics.getSimilarityDocsTime() / luceneShardMetrics.getAmountOfSimilarityDocsRequests();

                averageLuceneProxySimilarityScoresTime += averageSimilarityScoresTime;
                luceneProxySimilarityScoresAmount += luceneShardMetrics.getAmountOfSimilarityDocsRequests();
            }
            channel.shutdown();

            System.out.println("\n\nNode " + i + "has following statistics");
            System.out.println("\n\taverage search doc FAISS time: " + averageFaissSearchDocTime + ", requests amount: " + faissSearchDocAmount);
            System.out.println("\n\taverage similarity FAISS time: " + averageFaissSimilarityScoresTime + ", requests amount: " + faissSimilarityScoresAmount);
            System.out.println("\n\taverage similarity FAISS PROXY time: " + averageFaissProxySimilarityScoresTime + ", requests amount: " + faissProxySimilarityScoresAmount);
            System.out.println("\n\taverage search doc LUCENE time: " + averageLuceneSearchDocTime + ", requests amount: " + luceneSearchDocAmount);
            System.out.println("\n\taverage similarity LUCENE time: " + averageLuceneSimilarityScoresTime + ", requests amount: " + luceneSimilarityScoresAmount);
            System.out.println("\n\taverage similarity LUCENE PROXY time: " + averageLuceneProxySimilarityScoresTime + ", requests amount: " + luceneProxySimilarityScoresAmount);
            System.out.println("\n\toverall similarity requests amount: " + (faissProxySimilarityScoresAmount + luceneProxySimilarityScoresAmount + faissSimilarityScoresAmount + luceneSimilarityScoresAmount));
            System.out.println("\n\toverall search doc requests amount: " + (faissSearchDocAmount + luceneSearchDocAmount));
            System.out.println("\n\toverall requests amount: " + (faissSearchDocAmount + luceneSearchDocAmount + faissProxySimilarityScoresAmount + luceneProxySimilarityScoresAmount + faissSimilarityScoresAmount + luceneSimilarityScoresAmount));
        }
    }
}
