package cis5550.generic;

import static cis5550.webserver.Server.get;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

public class Coordinator {

	private static final Map<String, WorkerInfo> workers = new HashMap<>();
    private static final long TIMEOUT = 15000;

    public static void registerRoutes() {
        get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String port = req.queryParams("port");
            if (id == null || port == null) {
                res.status(400, "Bad Request: Missing id or port");
                return "FAIL";
            }
            
            String ip = req.ip();
            workers.put(id, new WorkerInfo(id, ip, port, System.currentTimeMillis()));
            return "OK";
        });
        
        get("/workers", (req, res) -> {
            return getWorkers();
        });
    }
    
    private static void cleanupInactiveWorkers() {
        long currentTime = System.currentTimeMillis();
        synchronized (workers) {
            Iterator<Map.Entry<String, WorkerInfo>> iterator = workers.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, WorkerInfo> entry = iterator.next();
                WorkerInfo worker = entry.getValue();
                if (currentTime - worker.lastPing > TIMEOUT) {
                    iterator.remove();
                }
            }
        }
    }

    public static String getWorkers() {
    	cleanupInactiveWorkers();
        StringBuilder sb = new StringBuilder();
        sb.append(workers.size()).append("\n");
        for (WorkerInfo worker : workers.values()) {
            sb.append(worker.id).append(",").append(worker.ip).append(":").append(worker.port).append("\n");
        }
        return sb.toString();
    }
    
    public static Vector<String> getWorkersList() {
    	cleanupInactiveWorkers();
        Vector<String> sb = new Vector<>();
        for (WorkerInfo worker : workers.values()) {
            sb.add(worker.ip + ":" + worker.port);
        }
        return sb;
    }

    public static String workerTable() {
    	cleanupInactiveWorkers();
        StringBuilder sb = new StringBuilder();
        sb.append("<tr><th>ID</th><th>IP:Port</th></tr>");
        for (WorkerInfo worker : workers.values()) {
            sb.append("<tr><td>").append(worker.id).append("</td><td><a href=\"http://")
                .append(worker.ip).append(":").append(worker.port).append("/\">")
                .append(worker.ip).append(":").append(worker.port).append("</a></td></tr>");
        }
        return sb.toString();
    }

    private static class WorkerInfo {
        String id, ip, port;
        long lastPing;

        WorkerInfo(String id, String ip, String port, long lastPing) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.lastPing = lastPing;
        }
    }
}