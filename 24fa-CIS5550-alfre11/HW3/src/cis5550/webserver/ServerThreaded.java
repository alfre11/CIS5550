package cis5550.webserver;

import cis5550.tools.Logger;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class ServerThreaded extends Thread {
	
	private static final Logger logger = Logger.getLogger(Server.class);
	private Socket clientSocket;
	private String path;
    private PrintWriter out;
    private Server serv;
	
	ServerThreaded(Socket clientSocket, String path, Server serv) {
		this.clientSocket = clientSocket;
		this.path = path;
		this.serv = serv;
	}
	
	
	public void run() {
//		System.out.println("We are here");
		
		try {
    		//previous server functionality
    		
    		out = new PrintWriter(clientSocket.getOutputStream());
    		
        	logger.info("Client connected");
    		
            //Read request and send the response
    		while (true) {
        		InputStream inStream = clientSocket.getInputStream();
            	OutputStream outStream = clientSocket.getOutputStream();
            	
            	ByteArrayOutputStream Buffy = new ByteArrayOutputStream();
            	int matchPtr = 0;
            	int bit;
            	int numBytes = 0;
                boolean headersComplete = false;
                boolean noMoreToRead = false;
                boolean routeBeenFound = false;
                boolean containsCookies = false;
                
                Map<String, String> headersMap = new HashMap<String, String>();
        		Map<String, String> paramsMap = new HashMap<String, String>();
        		Map<String, String> queryParamsMap = new HashMap<String, String>();
        		
        		SessionImpl currSesh = null;
        		
        		String contentType = "";

                while (matchPtr < 4) {
                	bit = inStream.read();
                	if(bit < 0) {
                		logger.info("end of request");
                		noMoreToRead = true;
                		break;
                	} else {
                		Buffy.write(bit);
                	    //entireBuffer.write(bit);
                		numBytes++;
                	}
                	//update match pointer
                	if ((((matchPtr==0) || (matchPtr==2)) && (bit=='\r')) || (((matchPtr==1) || (matchPtr==3)) && (bit=='\n'))) {
                		matchPtr++;
                	}
                    else {
                    	matchPtr = 0;
                    }
                	if (numBytes == 0) break;
                	if (matchPtr == 4) headersComplete = true;
                }
                
                if (noMoreToRead) {
                	logger.info("End of Reading");
                	break;
                }
                 
                if (!headersComplete) {
                	sendErrorResponse(outStream, "400 Bad Request");
                	logger.info("no CRLF found");
                    continue;
                }
                
            	
                String headers = Buffy.toString();
                String[] headerLines = headers.split("\r\n");
                
                //get request line
                String requestLine = headerLines[0];
                //System.out.println("Request line: " + requestLine);
                logger.info("Request line: " + requestLine);
                
                int contentLength = 0;
                for (String header : headerLines) {
                    if (header.startsWith("Content-Length:")) {
                        contentLength = Integer.parseInt(header.split(":")[1].trim());
                    }
                    String[] splitHeader = header.split(":");
                    logger.info("splitHeader[0]: " + splitHeader[0]);
                    if(splitHeader.length >= 2) {
                    	logger.info("splitHeader[1]: " + splitHeader[1]);
                    	headersMap.put(splitHeader[0].toLowerCase(), splitHeader[1]);
                    	
                    	if(splitHeader[0].equals("Content-Type")) {
                    		contentType = splitHeader[1];
                    		logger.info("content type- " + contentType);
                    	}
                    }    
                    if(header.startsWith("Cookie: ")) {
                    	containsCookies = true;
                    	String[] splitBySpace = header.split(" ");
                    	for(int i = 1; i < splitBySpace.length; i++) {
                    		if(splitBySpace[i].split("=")[0].equals("SessionID")) {
                        		String key = splitBySpace[i].split("=")[1];
//                        		if((System.currentTimeMillis() - Server.getSessionMap().get(key).lastAccessedTime() > ((SessionImpl) Server.getSessionMap().get(key)).maxInterval())) {
//                        			logger.info("server expired before parsing");
//                        			Server.getSessionMap().remove(key);
//                        		}
                        		if(((SessionImpl) Server.getSessionMap().get(key)).isExpired()) {
                        			logger.info("server expired before parsing");
                        			Server.getSessionMap().remove(key);
                        		}
                        		currSesh = (SessionImpl) Server.getSessionMap().get(key);
                        		logger.info("current session: " + currSesh);
                        		if(currSesh != null) {
                        			currSesh.setLastAccessed(System.currentTimeMillis());
                        		}	
                        		
                    		}
                    	}
                    }
                }
                
                
                String body = "";
                logger.info("content length: " + contentLength);
                //byte[] rawBody = new byte[contentLength];
                byte[] bodyBuffer = new byte[contentLength];
                //read content length
                if (contentLength > 0) {
                	//logger.info("If entered");
                	
                	int totalBodyBytesRead = 0;
                    int bodyBytesRead;
                    while (totalBodyBytesRead < contentLength && 
                            (bodyBytesRead = inStream.read(bodyBuffer, totalBodyBytesRead, contentLength - totalBodyBytesRead)) != -1) {
                    	totalBodyBytesRead += bodyBytesRead;
                    }
                     
                    if (totalBodyBytesRead == contentLength) {
                    	body = new String(bodyBuffer, 0, totalBodyBytesRead);
                    	logger.info("Body received: " + body);
                    } else {
                        logger.error("Failed to read the full body");
                    }
                }
                
                String[] requestSplit = requestLine.split(" ");
                if (requestSplit.length != 3) {
                    sendErrorResponse(outStream, "400 Bad Request");
                    logger.error("Malformed request line.");
                    continue;
                }

                String method = requestSplit[0];
                String httpVersion = requestSplit[2];

                
                //real response
                String[] splitReqLine = requestLine.split(" ");
                String command = splitReqLine[0];
                logger.info("command: " + command);
                String URL = splitReqLine[1];
                logger.info("requested URL: " + URL);
                String commandPath = path;

                Map<String, Route> routes = Server.getRoutes(method);
                
                String protocol = splitReqLine[2];
                String[] UrlSplit = URL.split("/");
                String[] UrlSplitNoQparam = UrlSplit;
                
        		
        		//check for qparams
        		String[] qparamSplit = URL.split("\\?");
        		String[] qparams;
        		
        		//contains qparams
        		if(qparamSplit.length > 1) {
        			logger.info("contains qparams");
        			UrlSplitNoQparam = qparamSplit[0].split("/");
        			qparams = qparamSplit[1].split("&");
        			for(int i = 0; i < qparams.length; i++) {
        				String[] paramParsed = qparams[i].split("=");
        				if(paramParsed.length == 2) {
        					queryParamsMap.put(URLDecoder.decode(paramParsed[0], "UTF-8"), URLDecoder.decode(paramParsed[1], "UTF-8"));
        				} else if(paramParsed.length == 1) {
        					queryParamsMap.put(URLDecoder.decode(paramParsed[0], "UTF-8"), "");
        				}
        				
        			}
        		}
        		
        		//body contains qparams
        		logger.info("Content Type: " + contentType);
        		if(contentType.equals(" application/x-www-form-urlencoded")) {
        			logger.info("body contains qparams");
        			String[] bodyQSplit = body.split("&");
        			for(int i = 0; i < bodyQSplit.length; i++) {
        				String[] paramParsed = bodyQSplit[i].split("=");
        				if(paramParsed.length == 2) {
        					queryParamsMap.put(URLDecoder.decode(paramParsed[0], "UTF-8"), URLDecoder.decode(paramParsed[1], "UTF-8"));
        				} else if(paramParsed.length == 1) {
        					queryParamsMap.put(URLDecoder.decode(paramParsed[0], "UTF-8"), "");
        				}
        			}
        		}
        		
                
                if(routes != null) {
                	for(String pattern : routes.keySet()) {
                		String[] patternSplit = pattern.split("/");
                		
                		if(patternSplit.length == UrlSplitNoQparam.length) {
                			logger.info("url matches route length");
                			logger.info(pattern + " : " + URL);
                			boolean patternMatching = true;
                			//loop through
                			for(int i = 0; i < UrlSplitNoQparam.length; i++) {
                				logger.info("match " + i + ": " + patternSplit[i] + ", " + UrlSplitNoQparam[i]);
                				if(!(patternSplit[i].startsWith(":"))) {
        							if(!patternSplit[i].equals(UrlSplitNoQparam[i])) {
        								logger.info("pattern does not match");
        								patternMatching = false;
        								break;
        							}
        						} else {
        							String patternParam = patternSplit[i];
        							if(patternSplit[i].startsWith(":")) {
        								patternParam = patternSplit[i].substring(1);
        							}
        							logger.info("param " + patternParam + " : " + UrlSplitNoQparam[i]);
        							paramsMap.put(patternParam, UrlSplitNoQparam[i]);
        						}
                				
                			}
                			
                			//matching pattern
        					if(patternMatching) {
        						//create new Request and Response Instance
        						routeBeenFound = true;
                    			logger.info("route pattern found: " + pattern);
                    			RequestImpl newRequest = new RequestImpl(method, URL, protocol, headersMap, queryParamsMap, paramsMap, 
                    					new InetSocketAddress(clientSocket.getInetAddress(), clientSocket.getPort()), bodyBuffer, serv, currSesh);
                    			ResponseImpl newResponse = new ResponseImpl(outStream);
                    			Route routeFound = routes.get(pattern);
                    			Object handleRet = null;
                    			String handRetString = "";
                    			try {
                    				handleRet = routeFound.handle(newRequest, newResponse);
                    				logger.info("return from handle: " + handleRet);
                    				logger.info("request handled");
                    			} catch(Exception e) {
                    				sendErrorResponse(outStream, "500 Internal Server Error");
                                    logger.error("Internal Server Error" + e);
                    			}
                    			
                    			logger.info("is there a new session: " + newRequest.getNewSession());
                    			if(newRequest.getNewSession()) {
                    				newResponse.header("Set-Cookie: ", "SessionID=" + newRequest.getSession().id());
                    			}
                    			
                    			if(!newResponse.getWriteCalled()) {
                    				
                    				//write headers
                    				out.print("HTTP/1.1 " + newResponse.getStatus() + " " + newResponse.getReason() + "\r\n");
                    				logger.info("Request Line: " + "HTTP/1.1 " + newResponse.getStatus() + " " 
                    						+ newResponse.getReason() + "\r\n");
                    				for(String header : newResponse.getHeader()) {
                    					logger.info("header: " + header);
                    					out.print(header + "\r\n");
                    				}
                    				out.print("\r\n");
                    				
                    				if(handleRet != null) {
                    					handRetString = handleRet.toString();
                    					//newResponse.body(handRetString);
                    					logger.info("body added from handle return: " + handRetString);
                    					out.print(handRetString);
                    					out.flush();
                    					
                    				} else {
                    					//write body
                    				logger.info("body: " + newResponse.getBody());
                    				out.print(newResponse.getBody());
                    				out.flush();
                    				logger.info("response sent");
                    				}
                    				
                    			}
                    			
        					}
                		}
                	
                		
                	}
                	if(routeBeenFound) {
                		out.close();
                		clientSocket.close();
                	}
                	
                }
                
                String newFilePath = commandPath + URL;
                logger.info("newFilePath: " + newFilePath);
                File file = new File(newFilePath);
                
                //Error handling
                
                if (method.equals("PUT") || method.equals("POST")) {
                    sendErrorResponse(outStream, "405 Method Not Allowed");
                    logger.error("method is not allowed.");
                    continue;
                }
                
                if (!(method.equals("PUT") || method.equals("POST") || method.equals("GET") || method.equals("HEAD"))) {
                    sendErrorResponse(outStream, "501 Not Implemented");
                    logger.error("method not implemented");
                    continue;
                }

                if (!httpVersion.equals("HTTP/1.1")) {
                    sendErrorResponse(outStream, "505 HTTP Version Not Supported");
                    logger.error("The HTTP version is not supported.");
                    continue;
                }
                
                
                if (!file.exists()) {
                	sendErrorResponse(outStream, "404 Not Found");
                	logger.error("Not Found");
                	continue;
                }
                
                if(!file.canRead() || URL.contains("..")) {
                	sendErrorResponse(outStream, "403 Forbidden");
                	logger.error("Forbidden");
                	continue;
                }
                
                
                //static response
                // Send response headers
                out.print("HTTP/1.1 200 OK\r\n");
                out.print("Content-Type: " + contentType + "\r\n");
                out.print("Server: Server\r\n");
                out.print("Content-Length: " + file.length() + "\r\n\r\n");
                out.flush();
                logger.info("file headers sent");
                
             // Send file data
                try (BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(file))) {
                    byte[] buffer = new byte[1024];
                    int bytesRead;
                    while ((bytesRead = fileInput.read(buffer)) != -1) {
                        outStream.write(buffer, 0, bytesRead);
                    }
                    out.flush();
                }
                
            	
        	}
    		 	out.close();
                clientSocket.close();
    		
    	} catch (Exception e) {
    		System.out.println("Error: " + e.getMessage());
            logger.error(e.getMessage());
    	}
		
		
	}
	
	
	// Method to send error responses
    private static void sendErrorResponse(OutputStream outSt, String statusCode) {
    	PrintWriter out = new PrintWriter(outSt, true);
            out.print("HTTP/1.1 " + statusCode + "\r\n");
            out.print("Content-Type: text/plain\r\n");
            out.print("Server: Server\r\n");
            out.print("Content-Length: " + statusCode.length() + "\r\n\r\n");
            out.print(statusCode);
            out.flush();
    }
}
