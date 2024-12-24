package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.*;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

public class PageRank {
	
	private static final Logger logger = Logger.getLogger(PageRank.class);
	
	public static void run(FlameContext ctx, String[] StringArg) {
		FlameRDD f = null;
		FlamePairRDD urrL = null;
		FlamePairRDD L = null;
		FlamePairRDD aggRanks = null;
		FlamePairRDD updated = null;
		FlameRDD rankDiffs = null;
		RowToString lamb = (r) -> {
			String url = r.get("url");
			String page = r.get("page");
			
			List<String> extr = new ArrayList<>(Arrays.asList(page.split("\n")));
			
			List<String> urls = extractURLs(page);
			StringBuilder urlsNormed = new StringBuilder();
			for(String u : urls) {
				String normedUrl = normalizeURLs(u, url);
				urlsNormed.append(normedUrl + ",");
			}
			
			
			if(!urlsNormed.toString().equals("")) {
				System.out.println(url + "," + "1.0,1.0," + urlsNormed.toString());
				return url + "," + "1.0,1.0," + urlsNormed.toString();
			} else {
				return null;
			}
			
		};
			
			try {
				f = ctx.fromTable("pt-crawl", lamb);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("exception with fromTable");
				e.printStackTrace();
			}
			
			if(f != null) {
				StringToPair lamb2 = (uP) -> {
					String[] split = uP.split(",", 2);
					FlamePair ret = new FlamePair(split[0], split[1]);
					return ret;
				};
				try {
					urrL = f.mapToPair(lamb2);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			boolean converged = false;
			double convThres = Double.parseDouble(StringArg[0]);
//			System.out.println("threshold: " + convThres);
			
			while(!converged) {
				PairToPairIterable lamb3 = (urrl) -> {
					String[] spl = urrl._2().split(",", 3);
					double currRank = Double.parseDouble(spl[0]);
					String urls = spl[2];
					String[] urlArray;
					if(!urls.isEmpty()) {
						urlArray = urls.split(",");
					} else {
						urlArray = new String[0];
					}
					
					List<FlamePair> ret = new ArrayList<>();
					
					if(urlArray.length > 0) {
						double contrib = 0.85 * currRank / urlArray.length;
						for(String link : urlArray) {
							FlamePair p = new FlamePair(link, Double.toString(contrib));
							ret.add(p);
//							System.out.println(p._1() + ", " + p._2());
						}
					}
					
					ret.add(new FlamePair(urrl._1(), "0.0"));
					return ret;
				};
				
				try {
					L = urrL.flatMapToPair(lamb3);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				TwoStringsToString lamb4 = (s1, s2) -> {
					double sum = Double.parseDouble(s1) + Double.parseDouble(s2);
					return String.valueOf(sum);
				};
				
				try {
					aggRanks = L.foldByKey("0.0", lamb4);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				try {
					updated = urrL.join(aggRanks);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				PairToPairIterable lamb5 = (pair) -> {
					String url = pair._1();
					String[] states = pair._2().split(",");
					double newR = 0;
					double alpha = 1 - 0.85;
					if(states == null) {
						return null;
					} else {
						newR = Double.parseDouble(states[states.length - 1]) + alpha;
					}
					StringBuilder buildNew = new StringBuilder();
					buildNew.append(newR + "," + states[0]);
					for(int i = 0; i < states.length - 3; i++) {
						buildNew.append("," + states[i+2]);
					}
					List<FlamePair> retList = new ArrayList<>();
					retList.add(new FlamePair(pair._1(), buildNew.toString()));
					return retList;
				};
				
				try {
					updated = updated.flatMapToPair(lamb5);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				PairToStringIterable findDiff = (pair) -> {
					String[] parts = pair._2().split(",", 3);
					double newRank = Double.parseDouble(parts[0]);
					double oldRank = Double.parseDouble(parts[1]);
					double diff = Math.abs(newRank - oldRank);
					return Collections.singletonList(String.valueOf(diff));
				};
				
				urrL = updated;
				
				try {
					rankDiffs = updated.flatMap(findDiff);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				TwoStringsToString findMax = (v1, v2) -> {
					double max = Math.max(Double.parseDouble(v1), Double.parseDouble(v2));
					return String.valueOf(max);
				};
				
				double maxDiff = 0;
				
				try {
					maxDiff = Double.parseDouble(rankDiffs.fold("0.0", findMax));
//					System.out.println("max diff: " + maxDiff);
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(maxDiff < convThres) {
					converged = true;
				}
			}
			
			PairToPairIterable backToTab = (pair) -> {
				String url = Hasher.hash(pair._1());
		        String[] stateParts = pair._2().split(",", 3);
		        double finalRank = Double.parseDouble(stateParts[0]);
		        Row newRow = new Row(url);
		        newRow.put("rank", Double.toString(finalRank));
		        ctx.getKVS().putRow("pt-pageranks", newRow);

		        return Collections.emptyList();
			};
			
			try {
				urrL.flatMapToPair(backToTab).saveAsTable("pt-pageranks");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("exception saving as pt-index table");
				e.printStackTrace();
			}
			
	}
	
	private static List<String> extractURLs(String content) {
		List<String> extractedUrls = new ArrayList<>();
		Pattern pattern = Pattern.compile("<a\\s+[^>]*href\\s*=\\s*['\"]([^'\"]+)['\"][^>]*>", Pattern.CASE_INSENSITIVE);
	    Matcher matcher = pattern.matcher(content);
	    while (matcher.find()) {
	        String url = matcher.group(1);
	        extractedUrls.add(url);
	    }
	    return extractedUrls;
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
