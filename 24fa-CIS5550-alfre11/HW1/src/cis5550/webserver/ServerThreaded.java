package cis5550.webserver;

import cis5550.tools.Logger;
import java.io.*;
import java.net.*;
import java.nio.file.Files;

public class ServerThreaded extends Thread {
	
	private static final Logger logger = Logger.getLogger(Server.class);
	private Socket clientSocket;
	private String path;
    private PrintWriter out;
	
	ServerThreaded(Socket clientSocket, String path) {
		this.clientSocket = clientSocket;
		this.path = path;
		
	}
	
	
	public void run() {
//		System.out.println("We are here");
		
		try {
    		//previous server functionality
    		
    		out = new PrintWriter(clientSocket.getOutputStream());
    		
//    		System.out.println("Client connected");
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
                	if ((((matchPtr==0) || (matchPtr==2)) && (bit=='\r')) || (((matchPtr==1) || (matchPtr==3)) && 								(bit=='\n'))) {
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
                
               
            	
                String headers = Buffy.toString("UTF-8");
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
                }
                
                
                
                logger.info("content length: " + contentLength);
                //read content length
                if (contentLength > 0) {
                	//logger.info("If entered");
                	byte[] bodyBuffer = new byte[contentLength];
                	int totalBodyBytesRead = 0;
                    int bodyBytesRead;
                    while (totalBodyBytesRead < contentLength && 
                            (bodyBytesRead = inStream.read(bodyBuffer, totalBodyBytesRead, contentLength - 								totalBodyBytesRead)) != -1) {
                    	totalBodyBytesRead += bodyBytesRead;
                    }
                     
                    if (totalBodyBytesRead == contentLength) {
                    	logger.info("Body received: " + new String(bodyBuffer, 0, totalBodyBytesRead));
                    } else {
                        logger.error("Failed to read the full body");
                    }
                }
                //logger.info("If exited");
                
                String[] requestSplit = requestLine.split(" ");
                if (requestSplit.length != 3) {
                    sendErrorResponse(outStream, "400 Bad Request");
                    logger.error("Malformed request line.");
                    continue;
                }

                String method = requestSplit[0];
                String httpVersion = requestSplit[2];

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
                
                
//                //dummy response
//                out.print("HTTP/1.1 200 OK\r\n");
//            	out.print("Content-Type: text/plain\r\n");
//            	out.print("Server: Server\r\n");
//            	out.print("Content-Length: 12\r\n\r\n");
//            	out.print("Hello World!");
//            	out.flush();
//            	logger.info("Response sent: Hello World!");
                
                //real response
                String[] splitReqLine = requestLine.split(" ");
                String command = splitReqLine[0];
                logger.info("command: " + command);
                String URL = splitReqLine[1];
                logger.info("requested URL: " + URL);
                String commandPath = path;
                String newFilePath = commandPath + URL;
                logger.info("newFilePath: " + newFilePath);
                File file = new File(newFilePath);
                
                if (!file.exists()) {
                	sendErrorResponse(outStream, "404 Not Found");
                	continue;
                }
                
                if(!file.canRead() || URL.contains("..")) {
                	sendErrorResponse(outStream, "403 Forbidden");
                	continue;
                }
                
                String contentType = Files.probeContentType(file.toPath());
                if (contentType == null) {
                	contentType = "application/octet-stream";
                }
                logger.info("content type: " + contentType);
                
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
