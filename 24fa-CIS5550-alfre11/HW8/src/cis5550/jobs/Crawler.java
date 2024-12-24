package cis5550.jobs;

import cis5550.flame.*;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.kvs.*;
import cis5550.tools.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {
	private static final Logger logger = Logger.getLogger(Crawler.class);
	
	public static void run(FlameContext ctx, String[] StringArg) {

		if(StringArg.length != 1) {
			ctx.output("Error: args - Crawler.run");
		} else {
			logger.info("good");
			ctx.output("OK\n");
			String url = StringArg[0];
			List<String> urlList = new ArrayList<>();
			urlList.add(url);
			FlameRDD urlQueue = null;
			try {
				urlQueue = ctx.parallelize(urlList);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("error creating url queue");
				e.printStackTrace();
			}
			if(urlQueue == null) {
				logger.info("null url queue");
				return;
			} else {
				try {
					while(urlQueue.count() > 0) {
						StringToIterable lambda = (u) -> {
							List<String> newUrlQueue = new ArrayList<>();
							URL page = new URL(u);
//							System.out.println("page url: " + page.toString());
							String pageNorm = normalizeURLs(page.toString(), page.toString());
//							System.out.println("page url normed: " + pageNorm);
							String urlHash = Hasher.hash(pageNorm);
							String host = page.getHost();
	                        Row row = new Row(urlHash);
	                        if (ctx.getKVS().existsRow("pt-crawl", urlHash)) {
//	                            System.out.println("Skipping already crawled URL: " + pageNorm);
	                            return newUrlQueue;
	                        }
	                        
	                        Row hostRow = ctx.getKVS().getRow("hosts", host);
	                        String robotsContent = hostRow != null ? hostRow.get("robots") : null;
	                        
	                        if (robotsContent == null) {
	                            HttpURLConnection robotsConn = (HttpURLConnection) new URL("http://" + host + "/robots.txt").openConnection();
	                            robotsConn.setRequestMethod("GET");
	                            robotsConn.setRequestProperty("User-Agent", "cis5550-crawler");
	                            robotsConn.connect();
	                            
	                            if (robotsConn.getResponseCode() == 200) {
	                                BufferedReader reader = new BufferedReader(new InputStreamReader(robotsConn.getInputStream()));
	                                StringBuilder robotsTxt = new StringBuilder();
	                                String line;
	                                while ((line = reader.readLine()) != null) {
	                                    robotsTxt.append(line).append("\n");
	                                }
	                                reader.close();
	                                robotsContent = robotsTxt.toString();
	                                
	                                //Save robots.txt in the hosts table
	                                if (hostRow == null) {
	                                    hostRow = new Row(host);
	                                }
	                                hostRow.put("robots", robotsContent);
	                                ctx.getKVS().putRow("hosts", hostRow);
	                            }
	                        }

	                        if (!isAllowedByRobots(page.getPath(), robotsContent)) {
	                            System.out.println("Blocked by robots.txt: " + pageNorm);
	                            return newUrlQueue;
	                        }
	                        
	                        long currentTime = System.currentTimeMillis();
	                        long lastAccessed = hostRow != null && hostRow.get("lastAccessed") != null 
	                                            ? Long.parseLong(hostRow.get("lastAccessed")) 
	                                            : 0;
//	                        System.out.println("current time: " + currentTime);
//	                        System.out.println("last accessed: " + lastAccessed);

	                        if (currentTime - lastAccessed < 1000) { // 1-second delay
//	                            System.out.println("Rate limit reached for host: " + host);
	                            newUrlQueue.add(pageNorm);
	                            return newUrlQueue;
	                        }

	                        Row updatedHostRow = new Row(host);
	                        updatedHostRow.put("lastAccessed", String.valueOf(currentTime));
	                        ctx.getKVS().putRow("hosts", updatedHostRow);
	                        
	                        HttpURLConnection headConn = (HttpURLConnection) page.openConnection();
	                        headConn.setRequestMethod("HEAD");
	                        headConn.setRequestProperty("User-Agent", "cis5550-crawler");
	                        headConn.setInstanceFollowRedirects(false);
	                        headConn.connect();

	                        int responseCode = headConn.getResponseCode();
	                        String contentType = headConn.getContentType();
	                        int contentLength = headConn.getContentLength();
	                        
	                        if (contentType != null) {
	                            row.put("contentType", contentType);
	                        }
	                        
	                        if(responseCode != 200) {
	                        	row.put("responseCode", String.valueOf(responseCode));
	                        }
	                        
	                        if(contentLength != -1) {
	                        	row.put("length", String.valueOf(contentLength));
	                        }
	                        
	                        if (responseCode == HttpURLConnection.HTTP_OK && 
	                                contentType != null && contentType.equalsIgnoreCase("text/html")) {
	                        	HttpURLConnection conn = (HttpURLConnection) page.openConnection();
								conn.setRequestMethod("GET");
								conn.setRequestProperty("User-Agent", "cis5550-crawler");
								conn.setInstanceFollowRedirects(false);
								conn.connect();
								
								if(conn.getResponseCode() == 200) {
									byte[] pageContentBytes = null;
									
									try (InputStream inputStream = conn.getInputStream();
									         ByteArrayOutputStream byteStream = new ByteArrayOutputStream()){
										byte[] buffer = new byte[1024];
								        int bytesRead;
								        while ((bytesRead = inputStream.read(buffer)) != -1) {
								            byteStream.write(buffer, 0, bytesRead);
								        }
								        pageContentBytes = byteStream.toByteArray();
								        
									} catch(IOException e) {
										logger.error("error reading page content" + e.getMessage());
									}
									
									String pageContent = new String(pageContentBytes, StandardCharsets.UTF_8);
									
//									BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//									StringBuilder pageContent = new StringBuilder();
//			                        String line;
//			                        while ((line = reader.readLine()) != null) {
//			                            pageContent.append(line).append("\n");
//			                        }
//			                        reader.close();
			                        List<String> urlsExtracted = extractURLs(pageContent);
//			                        List<String> parsedUrls = new ArrayList<>();
			                        
			                        for(String UrlEx : urlsExtracted) {
//			                        	System.out.println("url: " + UrlEx);
			                        	String parsedUrl = normalizeURLs(UrlEx, pageNorm);
			                        	if(parsedUrl != null) {
			                        		newUrlQueue.add(parsedUrl);
//			                        		System.out.println("normalized url: " + parsedUrl);
			                        	}
			                        }	             
			                        
			                        row.put("page", pageContent.toString());
			                        row.put("responseCode", String.valueOf(conn.getResponseCode()));
								}
	                        } else {
	                        	if(headConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_PERM || 
	                        			headConn.getResponseCode() == HttpURLConnection.HTTP_MOVED_TEMP || 
	                        			headConn.getResponseCode() == HttpURLConnection.HTTP_SEE_OTHER || 
	                        			headConn.getResponseCode() == 307 || 
	                        			headConn.getResponseCode() == 308) {
	                        		String newUrl = headConn.getHeaderField("Location");
//	                        		System.out.println("redirect detected, new url: " + newUrl);
	                        		String newUrlNormed = normalizeURLs(newUrl, pageNorm);
//	                        		row.put("url", pageNorm);
	                        		if(newUrlNormed != null) newUrlQueue.add(newUrlNormed);
	                        	}
	                        }
	                        row.put("url", pageNorm);
	                        ctx.getKVS().putRow("pt-crawl", row);
							return newUrlQueue;
						};
						
						urlQueue = urlQueue.flatMap(lambda);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
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
	
	private static boolean isAllowedByRobots(String path, String robotsContent) {
	    if (robotsContent == null) return true;
	    boolean userAgentMatched = false;
	    boolean isAllowed = true;
	    Pattern pattern = Pattern.compile("(?i)User-agent: (cis5550-crawler|\\*)|(?i)(Allow|Disallow): (.*)");
	    Matcher matcher = pattern.matcher(robotsContent);
	    while (matcher.find()) {
	        String userAgent = matcher.group(1);
	        String directive = matcher.group(2);
	        String rulePath = matcher.group(3);
	        if (userAgent != null) {
	            userAgentMatched = userAgent.equalsIgnoreCase("cis5550-crawler") || userAgent.equals("*");
	            continue;
	        }
	        if (userAgentMatched) {
	            if (directive.equalsIgnoreCase("Disallow") && path.startsWith(rulePath)) {
	                isAllowed = false;
	                break;
	            } else if (directive.equalsIgnoreCase("Allow") && path.startsWith(rulePath)) {
	                isAllowed = true;
	                break;
	            }
	        }
	    }
	    return isAllowed;
	}
}
