package cis5550.flame;

import java.util.*;
import java.util.function.Function;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.*;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {
	
	private static final Logger logger = Logger.getLogger(Worker.class);

	public static void main(String args[]) {
    if (args.length != 2) {
    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
    	System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    String server = args[1];
	  startPingThread(server, ""+port, port);
    final File myJAR = new File("__worker"+port+"-current.jar");

  	port(port);

    post("/useJAR", (request,response) -> {
      FileOutputStream fos = new FileOutputStream(myJAR);
      fos.write(request.bodyAsBytes());
      fos.close();
      return "OK";
    });
    
    post("/rdd/flatMap", (req, res) -> {
//    	logger.info("/rdd/flatMap entered");
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        byte[] serializedLambda = req.bodyAsBytes();
        String kvsCoord = req.queryParams("kvsCoordinator");

        StringToIterable lamb = (StringToIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");

            Iterable<String> results = lamb.op(value);

            if (results != null) {
                for (String result : results) {
                    String uniqueKey = UUID.randomUUID().toString();
                    kvs.put(outputTable, uniqueKey, "value", result);
                }
            }
        }
        return "OK";
    });
    
    post("/rdd/mapToPair", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        byte[] serializedLambda = req.bodyAsBytes();
        String kvsCoord = req.queryParams("kvsCoordinator");

        StringToPair lamb = (StringToPair) Serializer.byteArrayToObject(serializedLambda, myJAR);
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");

            if(value != null) {
            	FlamePair p = lamb.op(value);
            	kvs.put(outputTable, p.a, row.key(), p.b);
            }
        }
        return "OK";
    });
    
    post("/rdd/foldByKey", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        byte[] serializedLambda = req.bodyAsBytes();
        String kvsCoord = req.queryParams("kvsCoordinator");
        String zeroElement = req.queryParams("zeroElement");

        TwoStringsToString lamb = (TwoStringsToString) Serializer.byteArrayToObject(serializedLambda, myJAR);
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            String acc = zeroElement;

            for(String col : row.columns()) {
                String value = row.get(col);
                acc = lamb.op(acc, value);
            }
            kvs.put(outputTable, row.key(), "value", acc);
        }
        return "OK";
    });

	}
}
