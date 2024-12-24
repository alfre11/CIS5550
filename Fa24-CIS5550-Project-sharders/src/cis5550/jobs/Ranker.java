package cis5550.jobs;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;


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
                    logger.debug("trying to run query " + s);

                    FlameRDD urlTfIdfsForQuery = ctx.fromTable("pt-tfidf", row -> {
                        if(s.equals(row.key())) {
                            System.out.println("query: " + s + " tfidf: " + row.get("value"));
                            return row.get("value");
                        } else {
                            return null;
                        }
                    });

                    FlamePairRDD urlTfPairs = urlTfIdfsForQuery.mapToPair(acc -> {
                        String[] splitPairs = acc.split(";");
                        StringBuilder urlACC = new StringBuilder();
                        StringBuilder tfIdfACC = new StringBuilder();
                        for(String pair : splitPairs) {
                            String[] split = pair.split(",");
                            String url = URLDecoder.decode(split[0], "UTF-8");
                            String tfidf = split[1];

                            urlACC.append(Hasher.hash(url)).append(",");
                            tfIdfACC.append(tfidf).append(",");
                        }
                        urlACC.deleteCharAt(urlACC.length() - 1);
                        tfIdfACC.deleteCharAt(tfIdfACC.length() - 1);
                        return new FlamePair(urlACC.toString(), tfIdfACC.toString());
                    });

                    FlamePairRDD uniqueUrlTfPairs = urlTfPairs.flatMapToPair(pair -> {
                        String[] urls = pair._1().split(",");
                        String[] tfidfs = pair._2().split(",");

                        List<FlamePair> result = new ArrayList<>();

                        for (int i = 0; i < urls.length; i++) {
                            String url = urls[i].trim();
                            String tfidf = tfidfs[i].trim();

                            System.out.println("url: " + url + " tfidf: " + tfidf);
                            if(!url.isEmpty()) {
                                result.add(new FlamePair(url, tfidf));
                            }
                        }
                        return result;
                    });

                    FlamePairRDD foldedUnique = uniqueUrlTfPairs.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);

                    FlameRDD urlPageRanksForQuery = ctx.fromTable("pt-pageranks", row -> row.key() + "," + row.get("rank"));

                    FlamePairRDD urlPageRanks = urlPageRanksForQuery.mapToPair((p) -> {
                        String[] pairSplit = p.split(",");
                        String url = pairSplit[0];
                        String rank = pairSplit[1];
                        return new FlamePair(url, rank);
                    });

                    FlamePairRDD combinedScores = foldedUnique.join(urlPageRanks);

                    FlamePairRDD foldedCombined = combinedScores.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);

                    FlamePairRDD finalScores = foldedCombined.flatMapToPair(pair -> {
                        String url = pair._1();
                        String[] values = pair._2().split(",");
                        double tfidf = Double.parseDouble(values[0]);
                        double pagerank = Double.parseDouble(values[1]);

                        double combinedScore = tfidf * pagerank;
                        return Collections.singleton(new FlamePair(url, String.valueOf(combinedScore)));
                    });

                    FlamePairRDD foldedFinal = finalScores.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);

                    if(finalRanks == null) {
                        finalRanks = foldedFinal;
                    } else {
                        finalRanks = finalRanks.join(foldedFinal);
                    }

                } catch (Exception e) {
                    logger.error("Error reading from index table for queries");
                    throw new RuntimeException(e);
                }
            }
        }

        if(finalRanks != null) {
            try {
                finalRanks.saveAsTable("pt-finalranks");
                System.out.println("FINISHED");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}