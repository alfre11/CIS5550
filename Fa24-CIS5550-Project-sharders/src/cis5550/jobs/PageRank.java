package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {

	private static final double DECAY_FACTOR = 0.85;

	public static void run(FlameContext ctx, String[] args) throws Exception {
		double threshold = Double.parseDouble(args[0]);

		FlameRDD tempTable = ctx.fromTable("pt-crawl", row -> {
			String url = row.get("url");
			String pageContent = row.get("page");
			List<String> links = null;
			try {
				links = extractUrls(pageContent, url);
			} catch (Exception e) {e.printStackTrace();}
			String outLinks = String.join(",", links);
			return url + ",1.0,1.0," + outLinks;
		});

		FlamePairRDD stateTable = tempTable.mapToPair(s -> {
			String[] parts = s.split(",", 2);
			return new FlamePair(parts[0], parts[1]);
		});
		tempTable.destroy();

		while (true) {
			FlamePairRDD transferTable = stateTable.flatMapToPair(entry -> {
				String[] values = entry._2().split(",", 3);
				double currentRank = Double.parseDouble(values[0]);
				String[] links = values[2].split(",");
				double contribution = (links.length > 0) ? (DECAY_FACTOR * currentRank / links.length) : 0;

				List<FlamePair> contributions = Arrays.stream(links)
						.map(link -> new FlamePair(link, Double.toString(contribution)))
						.collect(Collectors.toList());

				contributions.add(new FlamePair(entry._1(), Double.toString(0.0)));
				return contributions;
			});

			FlamePairRDD newRanks = transferTable.foldByKey("0.0",
					(a,b) -> Double.toString(Double.parseDouble(a) + Double.parseDouble(b)));

			transferTable.destroy();
			
			stateTable = stateTable.join(newRanks);
			newRanks.destroy();
			
			stateTable = stateTable.flatMapToPair(entry -> {
				String[] values = entry._2().split(",");
				double newRank = Double.parseDouble(values[values.length - 1]) + (1.0 - DECAY_FACTOR);
				StringBuilder sb = new StringBuilder(newRank + "," + values[0]);
				for (int i = 2; i < values.length - 1; i++) {
					sb.append("," + values[i]);
				}
				return Collections.singletonList(
						new FlamePair(entry._1(), sb.toString())
				);
			});

			FlameRDD diffTable = stateTable.flatMap(entry -> {
				String[] values = entry._2().split(",");
				double currentRank = Double.parseDouble(values[0]);
				double prevRank = Double.parseDouble(values[1]);
				return Collections.singletonList(Double.toString(Math.abs(currentRank - prevRank)));
			});
      
			String maxDiff = diffTable.fold("0.0", (a,b) -> {
				if (!b.isEmpty()) return Double.toString(Math.max(Double.parseDouble(a), Double.parseDouble(b)));
				else return a;
			});
			diffTable.destroy();
			
			double maxChange = Double.parseDouble(maxDiff);
			if (maxChange < threshold) {
				break;
			}
		}

		stateTable.flatMapToPair(entry -> {
			String[] values = entry._2().split(",");
			double rank = Double.parseDouble(values[0]);
			String s = Hasher.hash(entry._1());
			Row r = new Row(s);
			r.put("rank", Double.toString(rank));
			ctx.getKVS().putRow("pt-pageranks", r);
			return new ArrayList<>();
		}).saveAsTable("pt-pageranks");
	}

	private static List<String> extractUrls(String content, String baseUrl) throws Exception {
		if (content == null) return new ArrayList<>();
		List<String> urls = new ArrayList<>();
		Pattern pattern = Pattern.compile("<a\\s+[^>]*href\\s*=\\s*['\"]([^'\"]*)['\"][^>]*>", Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(content);
		while (matcher.find()) {
			String rawUrl = matcher.group(1);
			urls.add(normalizeURL(rawUrl, baseUrl));
		}
		return urls;
	}

	private static String normalizeURL(String url) throws Exception {
		return normalizeURL(url, null);
	}

	public static String normalizeURL(String url, String baseUrl) {
		String[] parsedUrl = URLParser.parseURL(url);

		String scheme = parsedUrl[0];
		String host = parsedUrl[1];
		String port = parsedUrl[2];
		String path = parsedUrl[3];

		if (baseUrl != null) {
			String[] parsedBaseUrl = URLParser.parseURL(baseUrl);
			String baseScheme = parsedBaseUrl[0];
			String baseHost = parsedBaseUrl[1];
			String basePort = parsedBaseUrl[2];
			String basePath = parsedBaseUrl[3];

			if (scheme == null) scheme = baseScheme;
			if (host == null) host = baseHost;
			if (port == null) port = basePort != null ? basePort : (scheme.equals("https") ? "443" : "80");

			if (!path.startsWith("/")) {
				path = resolveRelativePath(basePath, path);
			}
		} else {
			if (scheme == null) scheme = "http";
			if (port == null) port = scheme.equals("https") ? "443" : "80";
		}

		int fragmentIndex = path.indexOf("#");
		if (fragmentIndex != -1) {
			path = path.substring(0, fragmentIndex);
		}

		StringBuilder normalizedUrl = new StringBuilder();
		normalizedUrl.append(scheme).append("://").append(host);
		normalizedUrl.append(":").append(port);

		normalizedUrl.append(path);
		return normalizedUrl.toString();
	}

	private static String resolveRelativePath(String basePath, String relativePath) {
		String[] baseSegments = basePath.split("/");
		String[] relativeSegments = relativePath.split("/");

		if (!basePath.endsWith("/")) {
			baseSegments = java.util.Arrays.copyOf(baseSegments, baseSegments.length - 1);
		}

		java.util.List<String> pathSegments = new java.util.ArrayList<>(java.util.Arrays.asList(baseSegments));

		for (String segment : relativeSegments) {
			if (segment.equals("..")) {
				if (!pathSegments.isEmpty()) {
					pathSegments.remove(pathSegments.size() - 1);
				}
			} else if (!segment.equals(".")) {
				pathSegments.add(segment);
			}
		}

		return String.join("/", pathSegments);
	}
}