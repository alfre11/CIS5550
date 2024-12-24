package cis5550.kvs;

import static cis5550.webserver.Server.get;
import static cis5550.webserver.Server.port;

public class Coordinator extends cis5550.generic.Coordinator {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        port(port);
        registerRoutes();
        get("/", (req, res) -> {
            return "<html><body><h1>KVS Coordinator</h1><table>" + workerTable() + "</table></body></html>";
        });
    }
}
