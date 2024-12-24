package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
	
    private String tableName;
    private FlameContextImpl context;

    public FlameRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }

    @Override
    public List<String> collect() throws Exception {
        Iterator<Row> rows = Coordinator.kvs.scan(tableName);
        List<String> result = new ArrayList<>();
        while (rows.hasNext()) {
        	Row row = rows.next();
            result.add(row.get("value"));
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String outputTable = "rdd_flatmap_" + UUID.randomUUID();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        context.invokeOperation("/rdd/flatMap", serializedLambda, tableName, outputTable, null);
        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String outputTable = "rdd_mapToPair_" + UUID.randomUUID();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        context.invokeOperation("/rdd/mapToPair", serializedLambda, tableName, outputTable, null);
        return new FlamePairRDDImpl(outputTable, context);
    }

	@Override
	public int count() throws Exception {
		return Coordinator.kvs.count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		Coordinator.kvs.rename(tableName, tableNameArg);
		tableName = tableNameArg;
	}

	@Override
	public FlameRDD distinct() throws Exception {
        String outputTable = "rdd_distinct_" + UUID.randomUUID();
        context.invokeOperation("/rdd/distinct", null, tableName, outputTable, null);
        return new FlameRDDImpl(outputTable, context);
	}

	@Override
	public void destroy() throws Exception {
		Coordinator.kvs.delete(tableName);
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		Iterator<Row> rows = Coordinator.kvs.scan(tableName);
        Vector<String> result = new Vector<>();
        for (int i = 0; i < num; i++) {
        	if (rows.hasNext()) result.add(rows.next().get("value"));
        	else break;
        }
        return result;
	}

	@Override
	public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
        return context.invokeFold(lambda, tableName, zeroElement);
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String outputTable = "rdd_flatmap_" + UUID.randomUUID();
        byte[] serializedLambda = Serializer.objectToByteArray(lambda);
        context.invokeOperation("/rdd/flatMapToPair", serializedLambda, tableName, outputTable, null);
        return new FlamePairRDDImpl(outputTable, context);
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        return null;
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return null;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }
}
