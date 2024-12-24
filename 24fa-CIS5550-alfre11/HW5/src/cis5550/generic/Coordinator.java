package cis5550.generic;

import java.util.*;

import cis5550.tools.Logger;
import cis5550.webserver.*;

public class Coordinator {
	
	private static final Logger logger = Logger.getLogger(Coordinator.class);
	int port;
	protected static final Map<String, infoForWorker> workers = new HashMap<>();
	
	protected static class infoForWorker {
		String id;
		String port;
		String ip;
		long lastPinged;
		
		infoForWorker(String id, String port, String ip, long lastPinged) {
			this.id = id;
			this.port = port;
			this.ip = ip;
			this.lastPinged = lastPinged;
		}
		
	}
	
	private static String getWorkers() {
		StringBuilder workerList = new StringBuilder();
		List<String> expiredWorkers = new ArrayList<>();
	    for (String id : workers.keySet()) {
	        if (workerExpired(id)) {
	            logger.info("Worker Expired: " + id);
	            expiredWorkers.add(id);
	        }
	    }
	    
	    for (String id : expiredWorkers) {
	        workers.remove(id);
	    }
		workerList.append(workers.size() + "\n");
		for(String id : workers.keySet()) {
			workerList.append(id).append(",").append(workers.get(id).ip).append(":").append(workers.get(id).port).append("\n");
		}
		return workerList.toString();
	}
	
	 public static String workerTable() {
		 StringBuilder table = new StringBuilder("<table><tr><th>ID</th><th>IP:Port</th></tr>");
		 workers.forEach((id, addr) -> table.append("<tr><td>").append(id).append("</td><td>")
				 .append("<a href=\"http://").append(addr).append("/\">").append(addr).append("</a>")
				 .append("</td></tr>"));
		 table.append("</table>");
		 return table.toString();
	 }
	
	protected static void registerRoutes() {
		Server.get("/ping", (req, res) -> {
			String id = req.queryParams("id");
            String port = req.queryParams("port");

            if (id == null || port == null) {
                res.status(400, "Bad Request"); 
                logger.error("400 Bad Request: Missing id or port");
                return "400 Bad Request: Missing id or port";
            }
            
            String ip = req.ip();
            
            logger.info("worker pinged: " + id);
            workers.put(id, new infoForWorker(id, port, ip, System.currentTimeMillis()));

            return "OK";
		});
		
		Server.get("/workers", (req, res) -> {
	         return getWorkers();
		});
	}
	
	protected static boolean workerExpired(String id) {
		return System.currentTimeMillis() - workers.get(id).lastPinged >= 15000;
	}
}
