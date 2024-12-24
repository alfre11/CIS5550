package cis5550.webserver;

import cis5550.tools.Logger;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Server extends Thread{
	
	private static final Logger logger = Logger.getLogger(Server.class);
	
	private static Server serv = null;
	
	private static boolean flag = false;
	
	private static int serverPort = 8080;
	
	private static String staticFilePath = null;
	
	//store routes
	private static final Map<String, Route> getRoutes = new HashMap<>();
	private static final Map<String, Route> postRoutes = new HashMap<>();
	private static final Map<String, Route> putRoutes = new HashMap<>();
	
	public static Map<String, Route> getRoutes(String command) {
		switch(command) {
    		case "GET":
    			return getRoutes;
    		case "POST":
    			return postRoutes;
    		case "PUT":
    			return putRoutes;
    		default:
    			return null;
    
		}
	}
	
	public void run() {
		//System.out.println("Server class run");
        
        try (ServerSocket serverSocket = new ServerSocket(serverPort)){
        	System.out.println("Server listening on port " + serverPort);
        	logger.info("Server listening");
        	while (true) {
        		Socket socket = null;
        		try {
        			socket = serverSocket.accept();
        			ServerThreaded thread = new ServerThreaded(socket, staticFilePath, serv);
        			thread.start();
        		} catch (Exception e) {
        			logger.error(e.getMessage());
        		}
        	}
        	
        } catch (IOException e) {
        	logger.error(e.getMessage());
        }
        
        System.out.print("Server class end");
    }
	
	
	public static class staticFiles {
		public static void location(String s) {
			staticFilePath = s;
			logger.info("Static files location set to: " + staticFilePath);
		}
	}
	
	public static void get(String s, Route r) {
		getRoutes.put(s, r);
		logger.info("GET route registered: " + s);
		if(serv == null) {
			serv = new Server();
		}
		if (!flag) {
			flag = true;
			Thread serverThread = new Thread(serv);
			serverThread.start();
		}
	}
	
	public static void post(String s, Route r) {
		postRoutes.put(s, r);
		logger.info("POST route registered: " + s);
		if(serv == null) {
			serv = new Server();
		}
		if (!flag) {
			flag = true;
			Thread serverThread = new Thread(serv);
			serverThread.start();
		}
	}

	public static void put(String s, Route r) {
		putRoutes.put(s, r);
		logger.info("PUT route registered: " + s);
		if(serv == null) {
			serv = new Server();
		}
		if (!flag) {
			flag = true;
			Thread serverThread = new Thread(serv);
			serverThread.start();
		}
	}
	
	public static void port(int i) {
		serverPort = i;
		logger.info("Port set to: " + serverPort);
		if(serv == null) {
			serv = new Server();
		}
	}
	
}
	

