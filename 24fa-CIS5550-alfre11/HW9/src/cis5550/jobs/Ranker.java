package cis5550.jobs;

import cis5550.flame.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import java.util.*;


public class Ranker {
    private static final Logger logger = Logger.getLogger(Ranker.class);

    static Map<String, Boolean> stopWords = new HashMap<>() {{
        put("i", true); put("me", true); put("my", true); put("myself", true); put("we", true); put("our", true); put("ours", true); put("ourselves", true);
        put("you", true); put("your", true); put("yours", true); put("yourself", true); put("yourselves", true); put("he", true); put("him", true);
        put("his", true); put("himself", true); put("she", true); put("her", true); put("hers", true); put("herself", true); put("it", true);
        put("its", true); put("itself", true); put("they", true); put("them", true); put("their", true); put("theirs", true); put("themselves", true);
        put("what", true); put("which", true); put("who", true); put("whom", true); put("this", true); put("that", true); put("these", true); put("those", true);
        put("am", true); put("is", true); put("are", true); put("was", true); put("were", true); put("be", true); put("been", true); put("being", true);
        put("have", true); put("has", true); put("had", true); put("having", true); put("do", true); put("does", true); put("did", true); put("doing", true);
        put("a", true); put("an", true); put("the", true); put("and", true); put("but", true); put("if", true); put("or", true); put("because", true);
        put("as", true); put("until", true); put("while", true); put("of", true); put("at", true); put("by", true); put("for", true); put("with", true);
        put("about", true); put("against", true); put("between", true); put("into", true); put("through", true); put("during", true); put("before", true);
        put("after", true); put("above", true); put("below", true); put("to", true); put("from", true); put("up", true); put("down", true); put("in", true);
        put("out", true); put("on", true); put("off", true); put("over", true); put("under", true); put("again", true); put("further", true); put("then", true);
        put("once", true); put("here", true); put("there", true); put("when", true); put("where", true); put("why", true); put("how", true); put("all", true);
        put("any", true); put("both", true); put("each", true); put("few", true); put("more", true); put("most", true); put("other", true); put("some", true);
        put("such", true); put("no", true); put("nor", true); put("not", true); put("only", true); put("own", true); put("same", true); put("so", true);
        put("than", true); put("too", true); put("very", true); put("s", true); put("t", true); put("can", true); put("will", true); put("just", true);
        put("should", true); put("now", true);
    }};

    public static void run(FlameContext ctx, String[] args) {
        logger.debug("Ranker Run");
        String query = args[0];
        if(query == null) {
            logger.error("No query specified");
            return;
        }
        String[] querySplit = query.split(" ");


        FlamePairRDD finalRanks = null;

        for(String s : querySplit) {
            if(!stopWords.containsKey(s)) {
                try {
//                    FlameRDD urlTfIdfsForQuery = ctx.fromTable("pt-tfidf", row -> {
//                        if(s.equals(row.key())) {
//                            return row.key() + "-" + row.get("acc");
//                        } else {
//                            return null;
//                        }
//                    });
                    logger.debug("trying to run query " + s);

                    FlameRDD urlTfIdfsForQuery = ctx.fromTable("pt-tfidf", row -> {
                        if(s.equals(row.key())) {
                            logger.debug("query: " + s + " tfidf: " + row.get("acc"));
                            return row.get("acc");
                        } else {
                            return null;
                        }
                    });

                    FlamePairRDD urlTfPairs = urlTfIdfsForQuery.mapToPair(acc -> {
                        String[] splitPairs = acc.split(";");
//                        if(splitPairs.length == 1) {
//                            logger.error("Invalid query syntax, not enough args: " + s);
//                        }
//                        if(splitPairs.length % 2 != 0) {
//                            logger.error("Invalid query syntax, not even pairs: " + s);
//                        }

                        StringBuilder urlACC = new StringBuilder();
                        StringBuilder tfIdfACC = new StringBuilder();
                        for(String pair : splitPairs) {
                            String[] split = pair.split(",");

                            String url = split[0];
                            String tfidf = split[1];

//                            logger.debug("url: " + url + " tfidf: " + tfidf);

                            //normalize url so it matches page rank table
                            String normedUrl = normalizeURLs(url, url);
//                            logger.debug("normed url: " + normedUrl);

                            urlACC.append(Hasher.hash(normedUrl)).append(",");
                            tfIdfACC.append(tfidf).append(",");
                        }
                        return new FlamePair(urlACC.toString(), tfIdfACC.toString());
                    });

//                    logger.debug("urlTfPairs: " + urlTfPairs.toString());

                    urlTfPairs.saveAsTable("debug_url_pairs");



//                    FlamePairRDD rowPair = urlTfIdfsForQuery.mapToPair(s1 -> {
//                        String[] spl = s.split("-");
//                        String word = spl[0];
//                        String urltfs = spl[1];
//                        return new FlamePair(word, urltfs);
//                    });

                    FlamePairRDD uniqueUrlTfPairs = urlTfPairs.flatMapToPair(pair -> {
                        // Split the URLs and TF-IDF values into arrays
                        String[] urls = pair._1().split(",");
                        String[] tfidfs = pair._2().split(",");

                        List<FlamePair> result = new ArrayList<>();

                        // Create a unique FlamePair for each URL and TF-IDF value
                        for (int i = 0; i < urls.length; i++) {
                            String url = urls[i].trim();
                            String tfidf = tfidfs[i].trim();

                            logger.debug("url: " + url + " tfidf: " + tfidf);
                            if(!url.isEmpty()) {
//                                String hashedUrl = Hasher.hash(url);
                                result.add(new FlamePair(url, tfidf));
                            }
                        }
                        return result;
                    });

                    FlamePairRDD foldedUnique = uniqueUrlTfPairs.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);

                    foldedUnique.saveAsTable("debug_unique_keys");

                    FlameRDD urlPageRanksForQuery = ctx.fromTable("pt-pageranks", row -> row.key() + "," + row.get("rank"));


                    FlamePairRDD urlPageRanks = urlPageRanksForQuery.mapToPair((p) -> {
                        String[] pairSplit = p.split(",");
                        String url = pairSplit[0];
                        String rank = pairSplit[1];
                        return new FlamePair(url, rank);
                    });

                    FlamePairRDD foldedPageRanks = urlPageRanks.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);

                    foldedPageRanks.saveAsTable("debug_pagerank_keys");

                    FlamePairRDD combinedScores = foldedUnique.join(urlPageRanks);

                    FlamePairRDD foldedCombined = combinedScores.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);

                    foldedCombined.saveAsTable("debug_combined_scores");

                    FlamePairRDD finalScores = foldedCombined.flatMapToPair(pair -> {
                        String url = pair._1();
                        String[] values = pair._2().split(",");
                        double tfidf = Double.parseDouble(values[0]);
                        double pagerank = Double.parseDouble(values[1]);

                        double combinedScore = tfidf * pagerank;

                        List<FlamePair> result = new ArrayList<>();
                        return Collections.singleton(new FlamePair(url, String.valueOf(combinedScore)));
                    });

                    FlamePairRDD foldedFinal = finalScores.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);

//                    logger.debug("Unique URL-TF Pairs: " + uniqueUrlTfPairs);
//                    logger.debug("URL Page Ranks: " + urlPageRanks.toString());
//                    logger.debug("Combined Scores Count: " + combinedScores);

                    if(finalRanks == null) {
                        finalRanks = foldedFinal;
                    } else {
                        finalRanks = finalRanks.join(foldedFinal);
                    }
//                    finalScores.saveAsTable("pt-finalranks");


                } catch (Exception e) {
                    logger.error("Error reading from index table for queries");
                    throw new RuntimeException(e);
                }
            }
        }

        if(finalRanks != null) {
            try {
                finalRanks.saveAsTable("pt-finalranks");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
//        if (!finalRanks.isEmpty()) {
//            FlamePairRDD allFinalRanks = ctx.parallelize(finalRanks);
//            try {
//                allFinalRanks.saveAsTable("pt-finalranks");
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    private static String normalizeURLs(String extractedUrls, String currentUrl) {
        String parsedUrl = "";
        if(extractedUrls.contains("#")) {
            extractedUrls = extractedUrls.substring(0, extractedUrls.indexOf("#"));
        }
        if(extractedUrls.isEmpty()) {
            return null;
        }
        String[] urlSplit = URLParser.parseURL(extractedUrls);
        String[] currSplit = URLParser.parseURL(currentUrl);
        if(urlSplit[0] == null) {
            parsedUrl += currSplit[0] + "://";
            String[] slashSplit = currentUrl.split(currentUrl);
            String fullUrl = urlSplit[3];
            if(fullUrl.startsWith("/")) {
                String currPort = currSplit[2];
                parsedUrl += currSplit[1] + ":";
                if(currPort != null) {
                    parsedUrl += currPort;
                } else {
                    if(currSplit[0].equals("https")) {
                        parsedUrl += "443";
                    } else {
                        parsedUrl += "80";
                    }
                }
                parsedUrl += fullUrl;
            } else {
                slashSplit[slashSplit.length-1] = fullUrl;
                return slashSplit.toString();
            }
        } else {
            parsedUrl += urlSplit[0] + "://";
            String newPort = urlSplit[2];
            parsedUrl += urlSplit[1] + ":";
            if(newPort == null) {
                if(currSplit[0].equals("https")) {
                    parsedUrl += "443";
                } else {
                    parsedUrl += "80";
                }
            } else {
                parsedUrl += newPort;
            }
            parsedUrl += urlSplit[3];
        }
        return parsedUrl;
    }

}