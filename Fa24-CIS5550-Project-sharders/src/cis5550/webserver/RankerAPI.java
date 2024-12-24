package cis5550.webserver;

import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlameSubmit;
import cis5550.jobs.Ranker;
import cis5550.flame.FlameContext;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;

import java.util.Iterator;

public class RankerAPI {

    private static final Logger logger = Logger.getLogger(RankerAPI.class);

    public static void main(String[] args) {
        Server.port(8080);

        //Define /ranks route
        Server.get("/ranks", (req, res) -> {
            //Extract the "query" parameter from the URL
            String query = req.queryParams("query");
            if (query == null || query.isEmpty()) {
                res.status(400, "Bad Request");
                return "Missing or empty query parameter!";
            }
            logger.debug("Received query: " + query);

            try {
                KVSClient testKvs = new KVSClient("localhost:8000");

                //Run Ranker
//                String output = FlameSubmit.submit("localhost:9000", "ranker.jar", "cis5550.jobs.Ranker",
//                        new String[] { query });
//
//                if (output == null)
//                    logger.error("Looks like we weren't able to submit 'ranker.jar'; the response code was "
//                            + FlameSubmit.getResponseCode() + ", and the output was:\n\n"
//                            + FlameSubmit.getErrorResponse());
//                System.out.println("passed? "+output);


                // Fetch the results from the `pt-finalranks` table
                String rankedResults = fetchRankedResults(testKvs); // Define this method
                logger.debug("rankedResults: " + rankedResults);

                res.status(200, "OK");
                res.type("application/json");
                return rankedResults;
            } catch (Exception e) {
                logger.error("Error processing query: " + e.getMessage());
                res.status(500, "Internal Server Error");
                return "Internal server error.";
            }
        });
    }

    //Helper function to fetch ranked results from the pt-finalranks table
    private static String fetchRankedResults(KVSClient kvs) throws Exception {
        //Fetch results pt-finalranks table
        //return results as a JSON string

        StringBuilder results = new StringBuilder("[");

        Iterator<Row> iter = kvs.scan("pt-finalranks", null, null);

        logger.debug("begin scan");

        if (!iter.hasNext()) {
            logger.error("No rows found in pt-finalranks. Is the KVSClient correctly configured?");
            return "[]";
        }

        while (iter.hasNext()) {
            Row row = iter.next();
            String url = row.key();
            String score = row.get("score");
            logger.debug("url: " + url + ", score: " + score);
            results.append("{\"url\": \"").append(url).append("\", \"rank\": ").append(score).append("},");
        }

        if (results.length() > 1) results.setLength(results.length() - 1); // Remove trailing comma
        results.append("]");
        return results.toString();
    }
}
