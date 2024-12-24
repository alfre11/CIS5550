package cis5550.test;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Logger;

import java.util.HashMap;
import java.util.Map;

public class FinalRanksTest {
    private static final Logger logger = Logger.getLogger(FinalRanksTest.class);
    public static void main(String[] args) {
        try {
            String tableName = "pt-finalranks";

//            (new KVSClient("localhost:8000")).delete(tableName);
            KVSClient kvs = new KVSClient("localhost:8000");
//            kvs.delete(tableName);
            HTTP.Response response = HTTP.doRequest("GET", "http://" + kvs.getWorkerAddress(0) + "/tables", null);
		    if (response != null) {
		        String[] listOfTables = new String(response.body()).split("\n");
		        for (String table: listOfTables) {
		            kvs.delete(table);
		        }
		    }

            // Define test data: URLs and their scores
            Map<String, Double> testData = new HashMap<>();

            testData.put("http://example1.com", 0.95);
            testData.put("http://example2.com", 0.85);
            testData.put("http://example3.com", 0.75);
            testData.put("http://example4.com", 0.65);
            testData.put("http://example5.com", 0.55);
            testData.put("http://example6.com", 0.99);
            testData.put("http://example7.com", 0.88);
            testData.put("http://example8.com", 0.72);
            testData.put("http://example9.com", 0.63);
            testData.put("http://example10.com", 0.59);
            testData.put("http://example11.com", 0.91);
            testData.put("http://example12.com", 0.82);
            testData.put("http://example13.com", 0.74);
            testData.put("http://example14.com", 0.68);
            testData.put("http://example15.com", 0.61);
            testData.put("http://example16.com", 0.97);
            testData.put("http://example17.com", 0.86);
            testData.put("http://example18.com", 0.79);
            testData.put("http://example19.com", 0.66);
            testData.put("http://example20.com", 0.62);
            testData.put("http://example21.com", 0.93);
            testData.put("http://example22.com", 0.81);
            testData.put("http://example23.com", 0.76);
            testData.put("http://example24.com", 0.69);
            testData.put("http://example25.com", 0.64);

            for (Map.Entry<String, Double> entry : testData.entrySet()) {
                String url = entry.getKey();
                Double score = entry.getValue();
                kvs.put(tableName, url, "score", score.toString());
                System.out.println("Added URL: " + url + " with score: " + score);
            }

            System.out.println("Test data successfully added to the pt-finalranks table.");
        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}
