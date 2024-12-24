package cis5550.webserver;

import java.net.*;
import java.io.*;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.tools.Logger;
import cis5550.webserver.ChildThread;

public class Server extends Thread {

    private static final Logger logger = Logger.getLogger(Server.class);
    static Server server = null;
    static Boolean started = false;

    private static Integer port = 80;
    private static Integer sPort;
    private static String path = null;

    private static HashMap<String, Route> routes = new HashMap<>();
    private static ArrayList<Filter> beforeFilters = new ArrayList<>();
    private static ArrayList<Filter> afterFilters = new ArrayList<>();

    // Did not finish
    private static ArrayList<String> hosts = new ArrayList<>();

    private static ConcurrentHashMap<String, SessionImpl> sessions = new ConcurrentHashMap<>();

    public static class staticFiles {
        public static void location(String s) {
            path = s;
            checkStart();
        }
    }

    public void run() {
        logger.debug("-----[ENTERING SERVER CODE]-----");

        // Create the server socket with try with resource block
        try {
            String pwd = "secret";
            String keyFile = "keystore.jks";
            File file = new File(keyFile);


            if (!file.exists()) {
                keyFile = "../keystore.jks";
            }

            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream(keyFile), pwd.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, pwd.toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
            ServerSocketFactory factory = sslContext.getServerSocketFactory();

            staticFiles.location(System.getProperty("user.dir"));

            // TODO: Maybe need to put this inside a try-with-resources block?
            ServerSocket serverSocket = new ServerSocket(port);
            // Start the HTTP server loop in a new thread
            Thread normalThread = new Thread(() -> {
                logger.debug("HTTP Server started on port " + port);
                serverLoop(serverSocket);
            });
            normalThread.start();

            // Start the HTTPS server loop in a new thread
            if (sPort != null) {
                ServerSocket serverSocketTLS = factory.createServerSocket(sPort);
                Thread sslThread = new Thread(() -> {
                    logger.debug("HTTPS Server started on port " + sPort);
                    serverLoop(serverSocketTLS);
                });
                sslThread.start();
            }

            // Start the session expiration checker
            ExpiryThread expiryThread = new ExpiryThread();
            expiryThread.start();

        } catch (KeyStoreException | UnrecoverableKeyException | NoSuchAlgorithmException | KeyManagementException |
                 CertificateException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void serverLoop(ServerSocket s) {
        try {
            // Keep the ServerSocket running to accept new requests and run them in parallel
            while (true) {
                Socket socket = s.accept();
                // Create a thread for each new request
                ChildThread childThread = new ChildThread(socket, path, routes, server);
                // Start the thread and let it run in parallel
                childThread.start();
            }
        } catch (IOException e) {
            logger.error("I/O Exception in serverLoop");
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static void get(String s, Route r) {
        checkStart();
        routes.put("GET " + s, r);
    }

    public static void post(String s, Route r) {
        checkStart();
        routes.put("POST " + s, r);
    }

    public static void put(String s, Route r) {
        checkStart();
        routes.put("PUT " + s, r);
    }

    public static void before(Filter before) {
        beforeFilters.add(before);
    }

    public static void after(Filter after) {
        afterFilters.add(after);
    }

    public ArrayList<Filter> getBeforeFilters() {
        return beforeFilters;
    }

    public ArrayList<Filter> getAfterFilters() {
        return afterFilters;
    }

    public static void port(int i) {
        port = i;
        checkStart();
    }

    public static void securePort(int portNum) {
        if (routes.isEmpty()) {
            sPort = portNum;
            checkStart();
        }
    }

    public static void addSession(String id, SessionImpl ses) {
        sessions.put(id, ses);
    }

    public static SessionImpl getSession(String id) {
        return sessions.get(id);
    }

    public static String[] activeSessions() {
        return sessions.keySet().toArray(new String[0]);
    }

    public static void removeSession(String id) {
        sessions.remove(id);
    }

    private static void checkStart() {
        if (server == null) {
            server = new Server();
        }
        if (!started) {
            logger.debug("Starting Server");
            started = true;
            server.start();
        }

    }

    public static void host(String h) {
        hosts.add(h);
    }


}