package cis5550.flame;

import static cis5550.webserver.Server.port;
import static cis5550.webserver.Server.post;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.UUID;

import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

class Worker extends cis5550.generic.Worker {

	public static void main(String args[]) {
	    if (args.length != 2) {
	    	System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
	    	System.exit(1);
	    }
	
	    int port = Integer.parseInt(args[0]);
	    String server = args[1];
	    String id = generateRandomID();
		  startPingThread(server, port, id);
	    final File myJAR = new File("__worker"+port+"-current.jar");
	
	  	port(port);
	
	    post("/useJAR", (request,response) -> {
	    	String kvs = request.queryParams("kvs");
	    	Coordinator.kvs = new KVSClient(kvs);
	    	
	      FileOutputStream fos = new FileOutputStream(myJAR);
	      fos.write(request.bodyAsBytes());
	      fos.close();
	      return "OK";
	    });
	
	    post("/rdd/flatMap", (req, res) -> {
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        byte[] lambdaBytes = req.bodyAsBytes();

	        StringToIterable lambda;
	        try {
	            lambda = (StringToIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }
	        
	        try {
	            Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
	            		.forEachRemaining(row -> {
	                String value = row.get("value");
	                if (value == null) return;

	                Iterable<String> results = null;
					try {
						results = lambda.op(value);
					} catch (Exception e) {
						System.err.println("Failed to perform lambda: " + e.getMessage());
					}
	                if (results != null) {
	                    results.forEach(result -> {
	                        String uniqueKey = "rdd_" + inputTable + UUID.randomUUID();
	                        try {
	                            Coordinator.kvs.put(outputTable, uniqueKey, "value", result);
	                        } catch (Exception e) {
	                            System.err.println("Failed to insert into output table: " + e.getMessage());
	                        }
	                    });
	                }
	            });
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }

	        res.status(200, "OK");
	        return "flatMap operation completed successfully.";
	    });
	    
	    post("/rdd/mapToPair", (req, res) -> {
	    	System.out.println("Begin mapToPair");
	    	
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        byte[] lambdaBytes = req.bodyAsBytes();
	    	
	        StringToPair lambda;
	        try {
	            lambda = (StringToPair) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }
	        

	        try {
	            Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
	            		.forEachRemaining(row -> {
	                String value = row.get("value");
	                if (value == null) return;

	                FlamePair results = null;
					try {
						results = lambda.op(value);
					} catch (Exception e) {
						System.err.println("Failed to perform lambda: " + e.getMessage());
					}
	                if (results != null) {
                        String uniqueKey = "rdd_" + inputTable + results.a + UUID.randomUUID();
                        try {
                            Coordinator.kvs.put(outputTable, results.a, uniqueKey, results.b);
                        } catch (Exception e) {
                            System.err.println("Failed to insert into output table: " + e.getMessage());
                        }
	                }
	            });
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }
	        
	        System.out.println("End mapToPair");
	        
	        res.status(200, "OK");
	        return "mapToPair operation completed successfully.";
	    });
	    
	    post("/rdd/foldByKey", (req, res) -> {
	    	System.out.println("Begin foldByKey");
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        String zeroElement = req.queryParams("zeroElement");
	        byte[] lambdaBytes = req.bodyAsBytes();
	    	
	        TwoStringsToString lambda;
	        try {
	            lambda = (TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }

	        try {
	            Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
	            		.forEachRemaining(row -> {
	            	String acc = zeroElement;
	                for (String c : row.columns()) {
	                	acc = lambda.op(acc, row.get(c));
	                }
	                try {
	                	Coordinator.kvs.put(outputTable, URLDecoder.decode(row.key(), "UTF-8"), "value", acc);
	                } catch (Exception e) {
                        System.err.println("Failed to insert into output table: " + e.getMessage());
                    }
	            });
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }

	        System.out.println("End foldByKey");
	        res.status(200, "OK");
	        return "foldByKey operation completed successfully.";
	    });
	    
	    post("/fromTable", (req, res) -> {
	    	System.out.println("Begin fromTable");
	    	
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        byte[] lambdaBytes = req.bodyAsBytes();
	        
	        RowToString lambda;
	        try {
	            lambda = (RowToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }
	        
	        try {
	            Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
	            		.forEachRemaining(row -> {
	            	String result = lambda.op(row);
	                try {
	                	if (result != null) {
	                		Coordinator.kvs.put(outputTable, URLDecoder.decode(row.key(), "UTF-8"), "value", result);
	                	}
	                } catch (Exception e) {
                        System.err.println("Failed to insert into output table: " + e.getMessage());
                    }
	            });
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }
	        
	        System.out.println("End fromTable");
	        
	        res.status(200, "OK");
	        return "mapToPair operation completed successfully.";
	    });
	    
	    post("/rdd/flatMapToPair", (req, res) -> {
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        byte[] lambdaBytes = req.bodyAsBytes();

	        StringToPairIterable lambda;
	        try {
	            lambda = (StringToPairIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }
	        
	        try {
	            Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
	            		.forEachRemaining(row -> {
	                String value = row.get("value");
	                if (value == null) return;

	                Iterable<FlamePair> results = null;
					try {
						results = lambda.op(value);
					} catch (Exception e) {
						System.err.println("Failed to perform lambda: " + e.getMessage());
					}
	                if (results != null) {
	                    results.forEach(result -> {
	                        String uniqueKey = "rdd_" + inputTable + UUID.randomUUID();
	                        try {
	                            Coordinator.kvs.put(outputTable, result.a, uniqueKey, result.b);
	                        } catch (Exception e) {
	                            System.err.println("Failed to insert into output table: " + e.getMessage());
	                        }
	                    });
	                }
	            });
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }

	        res.status(200, "OK");
	        return "flatMap operation completed successfully.";
	    });
	    
	    post("/rdd/pairFlatMap", (req, res) -> {
	    	System.out.println("Begin pairFlatMap");
	    	
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        byte[] lambdaBytes = req.bodyAsBytes();

	        PairToStringIterable lambda;
	        try {
	            lambda = (PairToStringIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }
	        
	        try {
	            Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
	            		.forEachRemaining(row -> {
	            	for (String c : row.columns()) {
		                String value = row.get(c);
		                if (value == null) return;
	
		                Iterable<String> results = null;
						try {
							results = lambda.op(new FlamePair(URLDecoder.decode(row.key(), "UTF-8"), value));
						} catch (Exception e) {
							System.err.println("Failed to perform lambda: " + e.getMessage());
						}
		                if (results != null) {
		                    results.forEach(result -> {
		                        String uniqueKey = "rdd_" + inputTable + UUID.randomUUID();
		                        try {
		                            Coordinator.kvs.put(outputTable, uniqueKey, "value", result);
		                        } catch (Exception e) {
		                            System.err.println("Failed to insert into output table: " + e.getMessage());
		                        }
		                    });
		                }
	            	}
	            });
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }

	    	System.out.println("End pairFlatMap");
	    	
	        res.status(200, "OK");
	        return "flatMap operation completed successfully.";
	    });
	    
	    post("/rdd/pairFlatMapToPair", (req, res) -> {
	    	System.out.println("Begin flatMapToPair");
	    	
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        byte[] lambdaBytes = req.bodyAsBytes();

	        PairToPairIterable lambda;
	        try {
	            lambda = (PairToPairIterable) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }
	        
	        try {
	            Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
	            		.forEachRemaining(row -> {
	            	for (String c : row.columns()) {
		                String value = row.get(c);
		                if (value == null) return;
	
		                Iterable<FlamePair> results = null;
						try {
							results = lambda.op(new FlamePair(URLDecoder.decode(row.key(), "UTF-8"), value));
						} catch (Exception e) {
							System.err.println("Failed to perform lambda: " + e.getMessage());
						}
		                if (results != null) {
		                    results.forEach(result -> {
		                        String uniqueKey = "rdd_" + inputTable + UUID.randomUUID();
		                        try {
		                            Coordinator.kvs.put(outputTable, result.a, uniqueKey, result.b);
		                        } catch (Exception e) {
		                            System.err.println("Failed to insert into output table: " + e.getMessage());
		                        }
		                    });
		                }
	            	}
	            });
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }

	        System.out.println("End flatMapToPair");
	        
	        res.status(200, "OK");
	        return "flatMap operation completed successfully.";
	    });
	    
	    post("/rdd/distinct", (req, res) -> {
	    	String inputTable = req.queryParams("inputTable");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        
	        try {
	        	Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
        			.forEachRemaining(row -> {
        			String value = row.get("value");
        			try {
        				Coordinator.kvs.put(outputTable, value, "value", value);
        			} catch (Exception e) {
                        System.err.println("Failed to insert into output table: " + e.getMessage());
                    }
    			});
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }
	        res.status(200, "OK");
	        return "distinct operation completed successfully.";
	    });
	    
	    post("/rdd/join", (req, res) -> {
	    	System.out.println("Begin join");
	    	String inputTable = req.queryParams("inputTable");
	    	String inputTable2 = req.queryParams("inputTable2");
	        String outputTable = req.queryParams("outputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	    	
	        try {
	        	Coordinator.kvs.scan(inputTable, keyStart, keyEnd)
        			.forEachRemaining(row -> {
        			try {
	        			Coordinator.kvs.scan(inputTable2, keyStart, keyEnd)
	        				.forEachRemaining(row2 -> {
	        				try {
		        				if (URLDecoder.decode(row.key(), "UTF-8").equals(URLDecoder.decode(row2.key(), "UTF-8"))) {
		        					for (String c1 : row.columns()) {
		        						String v1 = row.get(c1);
		        						for (String c2 : row2.columns() ) {
		        							String v2 = row2.get(c2);
		    		                        String uniqueKey = "rdd_" + inputTable + UUID.randomUUID();
		        		        			try {
		        		        				Coordinator.kvs.put(outputTable, URLDecoder.decode(row.key(), "UTF-8"), uniqueKey, v1 + "," + v2);
		        		        			} catch (Exception e) {
		        		                        System.err.println("Failed to insert into output table: " + e.getMessage());
		        		                    }
		        						}
		        					}
		        				}
	        				} catch (Exception e) {
	            	            res.status(500, "Internal Server Error");
	            	            e.printStackTrace();
	            	        }
	    				});
        			} catch (Exception e) {
        	            res.status(500, "Internal Server Error");
        	            e.printStackTrace();
        	        }
    			});
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }
	        
	        System.out.println("End join");
	        
	        res.status(200, "OK");
	        return "join operation completed successfully.";
	    });
	    
	    post("/rdd/fold", (req, res) -> {
	    	System.out.println("Begin fold");
	    	String inputTable = req.queryParams("inputTable");
	        String keyStart = req.queryParams("keyStart");
	        String keyEnd = req.queryParams("keyEnd");
	        String zeroElement = req.queryParams("zeroElement");
	        byte[] lambdaBytes = req.bodyAsBytes();
	    	
	        TwoStringsToString lambda;
	        try {
	            lambda = (TwoStringsToString) Serializer.byteArrayToObject(lambdaBytes, myJAR);
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            return "Error deserializing lambda: " + e.getMessage();
	        }
	        if (lambda == null) {
		        res.status(200, "OK");
	        	return "lambda was null";
	        }
	        

        	String acc = zeroElement;
	        try {
	            Iterator<Row> it = Coordinator.kvs.scan(inputTable, keyStart, keyEnd);
	            while (it.hasNext()) {
	            	Row r = it.next();
	            	String value = r.get("value");
	            	acc = lambda.op(acc, value);
	            }
	        } catch (Exception e) {
	            res.status(500, "Internal Server Error");
	            e.printStackTrace();
	            return "Error processing data: " + e.getMessage();
	        }

	        System.out.println("End fold");
	        
	        res.status(200, "OK");
	        return acc;
	    });
	}
}
