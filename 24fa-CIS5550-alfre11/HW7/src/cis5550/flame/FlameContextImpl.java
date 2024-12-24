package cis5550.flame;

import java.io.File;
import java.io.IOException;
import java.util.*;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;
import cis5550.tools.Partitioner.Partition;

public class FlameContextImpl implements FlameContext{
	
	private static final Logger logger = Logger.getLogger(FlameContextImpl.class);
	private StringBuilder outputs = new StringBuilder();
	String jarName;
	
	public FlameContextImpl(String jarName) {
		// TODO Auto-generated constructor stub
		this.jarName = jarName;
	}

	@Override
	public KVSClient getKVS() {
		// TODO Auto-generated method stub
		return Coordinator.kvs;
	}

	@Override
	public void output(String s) {
		// TODO Auto-generated method stub
		outputs.append(s);
	}
	
	public String getOut() {
		return outputs.toString();
	}

	@Override
	public FlameRDD parallelize(List<String> list) throws Exception {
		// TODO Auto-generated method stub
		String tableName = "table_" + System.currentTimeMillis();
		KVSClient kvs = Coordinator.kvs;
		
		for(int i = 0; i < list.size(); i++) {
			String key = UUID.randomUUID().toString();
			kvs.put(tableName, key, "value", list.get(i));
		}
		FlameRDDImpl newRDD = new FlameRDDImpl(tableName, this);
		
		return newRDD;
	}
	
	 public String invokeOperation(String operation, byte[] serializedLambda, String inputTable, String zeroElement, FlamePairRDDImpl other) throws Exception {
	        // Generate a unique table name for the output of this operation
	        String outputTable = "rdd_" + System.currentTimeMillis();
//	        logger.info("invoke operation called");

	        // Set up partitioning with Partitioner
	        Partitioner partitioner = new Partitioner();
	        KVSClient kvs = Coordinator.kvs;
	        for(int i = 0; i < kvs.numWorkers(); i++) {
	        	String workerID = kvs.getWorkerID(i);
	        	String workerAddr = kvs.getWorkerAddress(i);
	        	String nextWorkerID = (i < kvs.numWorkers() - 1) ? kvs.getWorkerID(i + 1) : null;
	        	partitioner.addKVSWorker(workerAddr, workerID, nextWorkerID);
	        }
	        partitioner.addKVSWorker(kvs.getWorkerAddress(kvs.numWorkers()-1), null, kvs.getWorkerID(0));
	        for(String w : Coordinator.getWorkers()) {
	        	partitioner.addFlameWorker(w);
	        }
	        Vector<Partition> parts = partitioner.assignPartitions();
	        List<Thread> threads = new ArrayList<>();
	        
	        for(Partition p : parts) {
	        	Thread t = new Thread(() -> {
	        		try {
	        			StringBuilder Url = new StringBuilder();
	        			Url.append("http://").append(p.assignedFlameWorker).append(operation)
	        			.append("?inputTable=").append(inputTable)
	        			.append("&outputTable=").append(outputTable)
	        			.append("&kvsWorker=").append(p.kvsWorker)
	        			.append("&kvsCoordinator=").append(kvs.getCoordinator());
	        			if(p.fromKey != null ) {
	        				Url.append("&startKey=").append(p.fromKey);
	        			}
	        			if(p.toKeyExclusive != null ) {
	        				Url.append("&endKey=").append(p.toKeyExclusive);
	        			}
	        			if(zeroElement != null) {
	        				Url.append("&zeroElement=").append(zeroElement);
	        			}
	        			if(other != null) {
	        				Url.append("&otherTable=").append(other.tableName);
	        			}
//	        			logger.info(Url.toString());
	        			HTTP.doRequest("POST", Url.toString(), serializedLambda);
	        		} catch (IOException e) {
	        			logger.error("error sending message to partition");
	        			e.printStackTrace();
	        		}
	        	}); 
	        	threads.add(t);
	        	t.start();
	        }
	        
	        for(Thread thread : threads) {
	        	try {
	        		thread.join();
	        	} catch(Exception e) {
	        		logger.error("error joining threads");
	        		e.printStackTrace();
	        	}
	        }
	        
	        if(operation.equals("/rdd/fold")) {
	        	Iterator<Row> rows = kvs.scan(outputTable);
	        	String acc = zeroElement;
	        	File myJAR = new File(jarName);
	        	TwoStringsToString lamb = (TwoStringsToString) Serializer.byteArrayToObject(serializedLambda, myJAR);
	        	while(rows.hasNext()) {
	        		Row row = rows.next();
	        		acc = lamb.op(acc, row.get("value"));
	        	}
	        	return acc;
	        	
	        } else {
		        return outputTable;
	        }
	        
	    }

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = invokeOperation("/rdd/fromTable", serLambda, tableName, null, null);
		return new FlameRDDImpl(tabName, this);
	}

}
