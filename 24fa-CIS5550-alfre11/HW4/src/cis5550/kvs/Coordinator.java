package cis5550.kvs;

import cis5550.webserver.*;
import cis5550.tools.Logger;

public class Coordinator extends cis5550.generic.Coordinator {
	
	private static final Logger logger = Logger.getLogger(Coordinator.class);
	
	public static void main(String[] args) {
		if (args.length != 1) {
            System.out.println("Usage: java cis5550.kvs.Coordinator- incorrect args");
            return;
        }

        int port = Integer.parseInt(args[0]);
        
        try {
        	System.out.println("running coordinator, port: " + port);
        	Server.port(port);
        	
        	registerRoutes();
        	
        	Server.get("/", (req, res) -> {
				res.body("<html><head><title>KVS Coordinator</title></head><body>" +
                        "<h1>KVS Coordinator</h1>" + 
                        Coordinator.workerTable() + 
                        "</body></html>"); return null;
			});
        	
        } catch(Exception e) {
        	logger.error("error constructing Coordinator");
			e.printStackTrace();
        }
        
	}
	
}
