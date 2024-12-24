package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
	
	private String tableName;
	private FlameContextImpl context;

    public FlamePairRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }
    
    public List<FlamePair> collect() throws Exception {
        Iterator<Row> rows = Coordinator.kvs.scan(tableName);
        List<FlamePair> result = new ArrayList<>();
        while (rows.hasNext()) {
        	Row row = rows.next();
        	Set<String> cols = row.columns();
        	for (String c : cols) {
        		result.add(new FlamePair(row.key(), row.get(c)));
        	}
        }
        return result;
    }

    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String outputTable = "rdd_foldByKey_" + UUID.randomUUID();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        context.invokeOperation("/rdd/foldByKey", serializedLambda, tableName, outputTable, zeroElement);
        return new FlamePairRDDImpl(outputTable, context);
    }
    
	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		Coordinator.kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;
	}
	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String outputTable = "rdd_pairflatmap_" + UUID.randomUUID();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        context.invokeOperation("/rdd/pairFlatMap", serializedLambda, tableName, outputTable, null);
        return new FlameRDDImpl(outputTable, context);
	}
	@Override
	public void destroy() throws Exception {
		Coordinator.kvs.delete(tableName);
	}
	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String outputTable = "rdd_pairflatmaptopair_" + UUID.randomUUID();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        context.invokeOperation("/rdd/pairFlatMapToPair", serializedLambda, tableName, outputTable, null);
        return new FlamePairRDDImpl(outputTable, context);
	}
	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String outputTable = "rdd_join_" + UUID.randomUUID();
        context.invokeOperation2("/rdd/join", null, tableName, ((FlamePairRDDImpl) other).tableName, outputTable, null);
        return new FlamePairRDDImpl(outputTable, context);
	}
	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
