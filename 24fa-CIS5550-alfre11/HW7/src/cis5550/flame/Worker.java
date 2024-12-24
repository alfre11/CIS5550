package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.*;
import cis5550.flame.FlameRDD.*;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

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
    
    post("/rdd/fromTable", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        byte[] serializedLambda = req.bodyAsBytes();
        String kvsCoord = req.queryParams("kvsCoordinator");

        RowToString lamb = (RowToString) Serializer.byteArrayToObject(serializedLambda, myJAR);
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            String val = lamb.op(row);
            kvs.put(outputTable, row.key(), "value", val);
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
    
    post("/rdd/flatMapToPair", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        byte[] serializedLambda = req.bodyAsBytes();
        String kvsCoord = req.queryParams("kvsCoordinator");

        StringToPairIterable lamb = (StringToPairIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");
            Iterable<FlamePair> results = lamb.op(value);

            if (results != null) {
                for (FlamePair result : results) {
                    kvs.put(outputTable, result.a, row.key(), result.b);
                }
            }
        }
        return "OK";
    });
    
    post("/pairrdd/flatMap", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        byte[] serializedLambda = req.bodyAsBytes();
        String kvsCoord = req.queryParams("kvsCoordinator");

        PairToStringIterable lamb = (PairToStringIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            
            for(String col : row.columns()) {
            	String value = row.get(col);
            	String uniqueKey = row.key();

                Iterable<String> results = lamb.op(new FlamePair(uniqueKey, value));
                
                if (results != null) {
                    for (String result : results) {
                    	String uniqueOutKey = UUID.randomUUID().toString();
                        kvs.put(outputTable, uniqueOutKey, "value", result);
                    }
                }
            }
            
            
        }
        return "OK";
    });
    
    post("/pairrdd/flatMapToPair", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        byte[] serializedLambda = req.bodyAsBytes();
        String kvsCoord = req.queryParams("kvsCoordinator");

        PairToPairIterable lamb = (PairToPairIterable) Serializer.byteArrayToObject(serializedLambda, myJAR);
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            String uniqueKey = row.key();
            
            for(String col : row.columns()) {
            	String value = row.get(col);
            	Iterable<FlamePair> results = lamb.op(new FlamePair(uniqueKey, value));

                if (results != null) {
                    for (FlamePair result : results) {
//                    	String uniqueOutKey = UUID.randomUUID().toString();
                        kvs.put(outputTable, result.a, uniqueKey, result.b);
                    }
                }
            }

            
        }
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
    
    post("/rdd/distinct", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        String kvsCoord = req.queryParams("kvsCoordinator");
        
        KVSClient kvs = new KVSClient(kvsCoord);

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");
            
            kvs.put(outputTable, value, "value", value);
        }
        return "OK";
    });
    
    post("/pairrdd/join", (req, res) -> {
        String inputTable = req.queryParams("inputTable");
        String outputTable = req.queryParams("outputTable");
        String startKey = req.queryParams("startKey");
        String endKey = req.queryParams("endKey");
        String kvsCoord = req.queryParams("kvsCoordinator");
        String otherTableName = req.queryParams("otherTable");
        
        KVSClient kvs = new KVSClient(kvsCoord);
        
        

        Iterator<Row> rows = kvs.scan(inputTable, startKey, endKey);
        while (rows.hasNext()) {
            Row row = rows.next();
            Row rowTab2 = kvs.getRow(otherTableName, row.key());
            
            if(rowTab2 != null) {
            	for(String col : row.columns()) {
            		for(String col2 : rowTab2.columns()) {
            			String uniqueColName = UUID.randomUUID().toString() + "_" + UUID.randomUUID().toString();
            			String value = row.get(col);
            			String value2 = rowTab2.get(col2);
                        kvs.put(outputTable, row.key(), uniqueColName, value + "," + value2);
            		}
            	}
            }
            
        }
        return "OK";
    });
    
    post("/rdd/fold", (req, res) -> {
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
        	String acc = zeroElement;
            Row row = rows.next();
            
            for(String col : row.columns()) {
                acc = lamb.op(acc, row.get(col));
            }
            kvs.put(outputTable, row.key(), "value", acc);
        }
        return "OK";
    });

	}
}
