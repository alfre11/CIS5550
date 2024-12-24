package cis5550.kvs;

import java.util.*;
import java.io.*;
import java.nio.file.Files;
import cis5550.webserver.Server;
import cis5550.tools.Logger;

public class Worker extends cis5550.generic.Worker {
    private static final Logger logger = Logger.getLogger(Server.class);
    private static Map<String, Map<String, Row>> tables = new HashMap<>();
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: Worker <port> <storageDir> <coordinatorIP:port>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        String storageDir = args[1];
        String coordinatorURL = "http://" + args[2];

        File idFile = new File(storageDir + "/id");
        String workerID;
        if (idFile.exists()) {
            workerID = new String(Files.readAllBytes(idFile.toPath())).trim();
        } else {
            workerID = generateRandomID();
            Files.write(idFile.toPath(), workerID.getBytes());
        }

        cis5550.generic.Worker.startPingThread(workerID, port, coordinatorURL);

        Server.port(port);
        Server.put("/data/:table/:row/:column", (req, res) -> {
            String table = req.params("table");
            String row = req.params("row");
            String column = req.params("column");
            byte[] data = req.bodyAsBytes();

            putRow(table, row, column, data);
            return "OK";
        });

        Server.get("/data/:table/:row/:column", (req, res) -> {
            String table = req.params("table");
            String row = req.params("row");
            String column = req.params("column");

            String data = getRow(table, row, column);
            if (data == null) {
                res.status(404, "Not Found");
            } else {
                res.body(data);
            }
            return null;
        });

    }

    private static String generateRandomID() {
        String alphabet = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            id.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return id.toString();
    }

    private static void putRow(String table, String row, String column, byte[] data) {
        tables.computeIfAbsent(table, k -> new HashMap<>());
        Row r = tables.get(table).computeIfAbsent(row, k -> new Row(k));
        r.put(column, data);
    }

    private static String getRow(String table, String row, String column) {
        if (!tables.containsKey(table)) return null;
        Row r = tables.get(table).get(row);
        if (r == null) return null;
        return r.get(column);
    }
}

