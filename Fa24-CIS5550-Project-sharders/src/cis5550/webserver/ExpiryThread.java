package cis5550.webserver;

import java.time.Duration;

import cis5550.tools.Logger;
import cis5550.webserver.Server;
import cis5550.webserver.SessionImpl;

public class ExpiryThread extends Thread {
    private static final Logger logger = Logger.getLogger(ExpiryThread.class);
    private static final long sleepms = 2500;

    @Override
    public void run() {
        while (true) {
            // Just use this thread to iterate through sessions in the server
            String[] sessions = Server.activeSessions();
            for (String id : sessions) {
                // Check if the session is expired, if so, remove it from the map
                SessionImpl s = Server.getSession(id);
                long delt = System.currentTimeMillis() - s.lastAccessedTime();
                if (delt > (s.getMaxActiveInterval() * 1000L)) {
                    Server.removeSession(id);
                    logger.debug("Expired: " + id);
                }
            }
            try {
                Thread.sleep(sleepms);
            } catch (InterruptedException e) {
                logger.error("Thread sleep throw an error: ", e);
                throw new RuntimeException(e);
            }
        }
    }
}
