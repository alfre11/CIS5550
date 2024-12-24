package cis5550.jobs;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cis5550.flame.*;
import cis5550.tools.Logger;

public class Indexer {
	private static final Logger logger = Logger.getLogger(Indexer.class);

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
		try {
			FlameRDD pages = ctx.fromTable("pt-crawl", row -> row.get("url") + "," + row.get("page"));

			int totalUrls = pages.count();

			FlamePairRDD urlPagePairs = pages.mapToPair(s -> {
				String[] parts = s.split(",", 2);
				return new FlamePair(parts[0], parts[1]);
			});
			pages.destroy();

			FlamePairRDD wordToTf = urlPagePairs.flatMapToPair(row -> {
				String url = row._1();
				String pageContent = row._2().toLowerCase();

				pageContent = pageContent.replaceAll("<[^>]*>", "");
				pageContent = pageContent.replaceAll("[.,:;!?'\"()-]", "");
				String[] words = pageContent.split("\\s+");

				Map<String, Integer> wordCount = new HashMap<>();
				for (String word : words) {
					if (!word.isEmpty() && !stopWords.containsKey(word) && word.matches("[a-zA-Z]+")) {
						wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
					}
				}

				List<FlamePair> result = new ArrayList<>();
				int totalWords = words.length;
				wordCount.forEach((word, count) -> {
					double tf = (double) count / totalWords;
					result.add(new FlamePair(word, url + "," + tf));
				});

				return result;
			});
			urlPagePairs.destroy();
			
			FlamePairRDD documentFrequency = wordToTf.foldByKey("0", (accumulator, current) -> {
				int acc = Integer.parseInt(accumulator);
				return String.valueOf(acc + 1);
			});

			FlamePairRDD tfDfJoined = wordToTf.join(documentFrequency);

			wordToTf.destroy();
			documentFrequency.destroy();
			
			FlamePairRDD tfIdf = tfDfJoined.flatMapToPair(pair -> {
				String word = pair._1();
				String[] values = pair._2().split(",", 3);

				String url = values[0];
				double tf = Double.parseDouble(values[1]);
				int df = Integer.parseInt(values[2]);

				double idf = Math.log((double) totalUrls / df);
				double tfIdfScore = tf * idf;

				List<FlamePair> result = new ArrayList<>();
				result.add(new FlamePair(word, URLEncoder.encode(url, "UTF-8") + "," + tfIdfScore));
				return result;
			});

			FlamePairRDD aggregatedTfIdf = tfIdf.foldByKey("", (acc, value) -> acc.isEmpty() ? value : acc + ";" + value);
			tfIdf.destroy();
			
			aggregatedTfIdf.saveAsTable("pt-tfidf");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}