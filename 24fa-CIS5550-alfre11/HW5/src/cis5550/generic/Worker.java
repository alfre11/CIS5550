package cis5550.generic;

import java.net.URL;
import cis5550.webserver.Server;
import cis5550.tools.Logger;

public class Worker {
    private static final Logger logger = Logger.getLogger(Server.class);
    
    public static void startPingThread(String workerID, int port, String coordinatorURL) {
        Thread pingThread = new Thread(() -> {
            while (true) {
                try {
                    String pingURL = coordinatorURL + "/ping?id=" + workerID + "&port=" + port;
                    URL url = new URL(pingURL);
                    url.getContent();
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        pingThread.start();
    }

}
