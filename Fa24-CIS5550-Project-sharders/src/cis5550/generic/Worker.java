package cis5550.generic;

import java.net.HttpURLConnection;
import java.net.URL;

public class Worker {

    public static void startPingThread(String coordinatorURL, int port, String id) {
        Thread pingThread = new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(5000);

                    String pingURL = "http://" + coordinatorURL + "/ping?id=" + id + "&port=" + port;
                    URL url = new URL(pingURL);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");
                    connection.getInputStream().close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        pingThread.start();
    }

    protected static String generateRandomID() {
        String letters = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder id = new StringBuilder(5);
        for (int i = 0; i < 5; i++) {
            int index = (int) (Math.random() * letters.length());
            id.append(letters.charAt(index));
        }
        return id.toString();
    }
}
