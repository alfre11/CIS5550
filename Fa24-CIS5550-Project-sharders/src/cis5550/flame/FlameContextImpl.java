package cis5550.flame;

import java.io.Serializable;
import java.net.URLEncoder;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;

public class FlameContextImpl implements FlameContext, Serializable {
	
	private String name;
    private StringBuilder outputStorage = new StringBuilder();
    
    public FlameContextImpl(String name) {
    	this.name = name;
    }

    public KVSClient getKVS() {
        return Coordinator.kvs;
    }

    public void output(String s) {
        if (s != null) {
            outputStorage.append(s);
        }
    }

    public String getOutput() {
        return outputStorage.toString();
    }

    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = "rdd_" + name + "_" + UUID.randomUUID();
        for (int i = 0; i < list.size(); i++) {
            String rowKey = Hasher.hash(Integer.toString(i));
            Coordinator.kvs.put(tableName, rowKey, "value", list.get(i));
        }
        return new FlameRDDImpl(tableName, this);
    }
    
    public void invokeOperation2(String operationName, byte[] lambda, String inputTable1, String inputTable2, String outputTable, String zeroElement) throws Exception {
    	Partitioner partitioner = new Partitioner();
    	int numWorkers = Coordinator.kvs.numWorkers();

    	List<String[]> kvsWorkers = new Vector<>();
    	for (int i = 0; i < numWorkers; i++) {
    	    String[] kvsWorker = {Coordinator.kvs.getWorkerAddress(i), Coordinator.kvs.getWorkerID(i)};
    	    kvsWorkers.add(kvsWorker);
    	}

    	kvsWorkers.sort((worker1, worker2) -> worker1[1].compareTo(worker2[1]));
    	for (int i = 0; i < numWorkers - 1; i++) {
    	    partitioner.addKVSWorker(kvsWorkers.get(i)[0], kvsWorkers.get(i)[1], kvsWorkers.get(i + 1)[1]);
    	}

    	if (numWorkers > 0) {
    	    partitioner.addKVSWorker(kvsWorkers.get(numWorkers - 1)[0], null, kvsWorkers.get(0)[1]);
    	    partitioner.addKVSWorker(kvsWorkers.get(numWorkers - 1)[0], kvsWorkers.get(numWorkers - 1)[1], null);
    	}
    	Coordinator.getWorkersList().forEach(name -> {
    	    partitioner.addFlameWorker(name);
    	});

    	List<Partitioner.Partition> partitions = partitioner.assignPartitions();

        Thread threads[] = new Thread[partitions.size()];
        String results[] = new String[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
        	Partitioner.Partition partition = partitions.get(i);
            final int j = i;
            threads[i] = new Thread(() -> {
                try {
                	String url = "http://" + partition.assignedFlameWorker + operationName + createRequestParams(inputTable1, inputTable2, outputTable, partition, zeroElement);
                	results[j] = new String(HTTP.doRequest("POST", url, lambda).body());
                } catch (Exception e) {
                	results[j] = "";
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }
        
        for (int i = 0; i < threads.length; i++) {
            try {
            	threads[i].join();
            } catch (InterruptedException ie) {
            	ie.printStackTrace();
            }
        }
    }
    
    public void invokeOperation(String operationName, byte[] lambda, String inputTable, String outputTable, String zeroElement) throws Exception {
    	invokeOperation2(operationName, lambda, inputTable, null, outputTable, zeroElement);
    }
    
    public String invokeFold(TwoStringsToString lambda, String inputTable, String zeroElement) throws Exception {
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        
    	Partitioner partitioner = new Partitioner();
    	int numWorkers = Coordinator.kvs.numWorkers();

    	List<String[]> kvsWorkers = new Vector<>();
    	for (int i = 0; i < numWorkers; i++) {
    	    String[] kvsWorker = {Coordinator.kvs.getWorkerAddress(i), Coordinator.kvs.getWorkerID(i)};
    	    kvsWorkers.add(kvsWorker);
    	}

    	kvsWorkers.sort((worker1, worker2) -> worker1[1].compareTo(worker2[1]));
    	for (int i = 0; i < numWorkers - 1; i++) {
    	    partitioner.addKVSWorker(kvsWorkers.get(i)[0], kvsWorkers.get(i)[1], kvsWorkers.get(i + 1)[1]);
    	}

    	if (numWorkers > 0) {
    	    partitioner.addKVSWorker(kvsWorkers.get(numWorkers - 1)[0], null, kvsWorkers.get(0)[1]);
    	    partitioner.addKVSWorker(kvsWorkers.get(numWorkers - 1)[0], kvsWorkers.get(numWorkers - 1)[1], null);
    	}
    	Coordinator.getWorkersList().forEach(name -> {
    	    partitioner.addFlameWorker(name);
    	});

    	List<Partitioner.Partition> partitions = partitioner.assignPartitions();

        Thread threads[] = new Thread[partitions.size()];
        String results[] = new String[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
        	Partitioner.Partition partition = partitions.get(i);
            final int j = i;
            threads[i] = new Thread(() -> {
                try {
                	String url = "http://" + partition.assignedFlameWorker + "/rdd/fold" + createRequestParams(inputTable, null, null, partition, zeroElement);
                	results[j] = new String(HTTP.doRequest("POST", url, serializedLambda).body());
                } catch (Exception e) {
                	results[j] = "";
                    e.printStackTrace();
                }
            });
            threads[i].start();
        }
        
        for (int i = 0; i < threads.length; i++) {
            try {
            	threads[i].join();
            } catch (InterruptedException ie) {
            	ie.printStackTrace();
            }
        }
        
        String acc = zeroElement;
        for (String s : results) {
        	acc = lambda.op(acc, s);
        }
        return acc;
    }
    
    private String createRequestParams(String inputTable1, String inputTable2, String outputTable, Partitioner.Partition partition, String zeroElement) throws Exception {
        return "?inputTable=" + URLEncoder.encode(inputTable1, "UTF-8") +
                (outputTable == null ? "" : "&outputTable=" + URLEncoder.encode(outputTable, "UTF-8")) +
                (inputTable2 == null ? "" : "&inputTable2=" + URLEncoder.encode(inputTable2, "UTF-8")) +
                (partition.fromKey == null ? "" : "&keyStart=" + URLEncoder.encode(partition.fromKey, "UTF-8")) +
                (partition.toKeyExclusive == null ? "" : "&keyEnd=" + URLEncoder.encode(partition.toKeyExclusive, "UTF-8")) +
                (zeroElement == null ? "" : "&zeroElement=" + URLEncoder.encode(zeroElement, "UTF-8"));
    }

	@Override
	public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String outputTable = "context_fromTable_" + UUID.randomUUID();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        invokeOperation("/fromTable", serializedLambda, tableName, outputTable, null);
		return new FlameRDDImpl(outputTable, this);
	}
}
