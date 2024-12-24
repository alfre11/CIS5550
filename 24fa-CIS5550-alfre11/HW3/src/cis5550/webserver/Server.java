package cis5550.webserver;

import cis5550.tools.Logger;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.*;
import java.security.*;
import java.security.cert.CertificateException;


public class Server extends Thread{
	
	private static final Logger logger = Logger.getLogger(Server.class);
	
	private static Server serv = null;
	
	private static boolean flag = false;
	
	private static int serverPort = 8080;
	
	private static int securePortNum = -1;
	
	private static boolean secure = false;
	
	private static String staticFilePath = null;
	
	//store routes
	private static final Map<String, Route> getRoutes = new HashMap<>();
	private static final Map<String, Route> postRoutes = new HashMap<>();
	private static final Map<String, Route> putRoutes = new HashMap<>();
	
	private static Map<String, Session> sessionMap = new ConcurrentHashMap<>();
	
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
		
		
		Thread secureThread = new Thread(() -> {
			String pwd = "secret";
			try {
				KeyStore keyStore = KeyStore.getInstance("JKS");
				keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
				KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
				keyManagerFactory.init(keyStore, pwd.toCharArray());
				SSLContext sslContext = SSLContext.getInstance("TLS");
				sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
				SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
				ServerSocket serverSocketTLS = factory.createServerSocket(securePortNum);
				
				System.out.println("Server listening on port " + securePortNum);
	        	logger.info("Server listening");
				
				while (true) {
	        		serverLoop(serverSocketTLS);
	        	}
				
			} catch(Exception e) {
				logger.error(e.getMessage());
			}
		});
		
		
		Thread nonSecureThread = new Thread(() -> {
			try (ServerSocket serverSocket = new ServerSocket(serverPort)){
	        	System.out.println("Server listening on port " + serverPort);
	        	logger.info("Server listening");
	        	while (true) {
	        		serverLoop(serverSocket );
	        	}
	        	
	        } catch (IOException e) {
	        	logger.error(e.getMessage());
	        }
			
		});
		
		Thread expireServers = new Thread(() -> {
			
			
			while(true) {
				try {
				Thread.sleep(5000);
				expireSessions();
				} catch (InterruptedException e) {
				e.printStackTrace();
				}
			}
			
		});
		
		if(secure) {
			secureThread.start();
		}
		
		nonSecureThread.start();
		expireServers.start();
		
		try {
			secureThread.join();
			nonSecureThread.join();
			expireServers.join();
				
		} catch(Exception e) {
			logger.error(e.getMessage());
		}
		
//        System.out.print("Server class end");
    }
	
	public void serverLoop(ServerSocket sock) {
		Socket socket = null;
		try {
			socket = sock.accept();
			ServerThreaded thread = new ServerThreaded(socket, staticFilePath, serv);
			thread.start();
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
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
	
	public static void securePort(int i) {
		secure = true;
		securePortNum = i;
		logger.info("secure port number set to: " + i);
		if(serv == null) {
			serv = new Server();
		}
	}

	public static Map<String, Session> getSessionMap() {
		return sessionMap;
	}

	public static void setSessionMap(Map<String, Session> sessionMap) {
		Server.sessionMap = sessionMap;
	}
	
	private void expireSessions() {
		logger.info("checking exipred");
		for(String sessionID : sessionMap.keySet()) {
			SessionImpl curr = (SessionImpl) sessionMap.get(sessionID);
			logger.info("checking session expired: " + sessionID);
			if(curr != null && curr.isExpired()) {
				logger.info("server expired: " + sessionID);
				sessionMap.remove(sessionID);
			}
			if(curr != null && !curr.isValid()) {
				logger.info("server invalid: " + sessionID);
				sessionMap.remove(sessionID);
			}
		}
	}
	
}
	

