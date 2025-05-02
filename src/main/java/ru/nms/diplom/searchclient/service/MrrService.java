package ru.nms.diplom.searchclient.service;

import ru.nms.diplom.searchgateway.service.Document;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class MrrService {
    public double computeMRR(Map<Integer, List<Document>> data, double p, double w) {
        double totalMRR = 0.0;

        for (Map.Entry<Integer, List<Document>> entry : data.entrySet()) {
            int relevantPassageId = entry.getKey();
            List<Document> docs = new ArrayList<>(entry.getValue());

            // Compute final scores and sort in descending order
            docs.sort(Comparator.comparingDouble((Document document) -> -(p * document.getFaissScore() + w * document.getLuceneScore())));

            // Find rank of the relevant passage
            for (int i = 0; i < docs.size(); i++) {
                if (docs.get(i).getId() == relevantPassageId) {
                    totalMRR += 1.0 / (i + 1);
                    break;
                }
            }
        }

        return totalMRR / data.size();
    }

    public List<Document> normalizeScores(List<Document> data) {

        float maxLuceneScore = data.stream().map(Document::getLuceneScore).max(Float::compare).get();
        float minLuceneScore = data.stream().map(Document::getLuceneScore).min(Float::compare).get();

        float maxFaissDistance = data.stream().map(Document::getFaissScore).max(Float::compare).get();
        float minFaissDistance = data.stream().map(Document::getFaissScore).min(Float::compare).get();

        var result = new ArrayList<Document>();
        data.forEach(d -> result.add(d.toBuilder()
                .setFaissScore(1 - (d.getFaissScore() - minFaissDistance) / (maxFaissDistance - minFaissDistance))
                .setLuceneScore((d.getLuceneScore() - minLuceneScore) / (maxLuceneScore - minLuceneScore))
                .build()
        ));
        return result;
    }
}
