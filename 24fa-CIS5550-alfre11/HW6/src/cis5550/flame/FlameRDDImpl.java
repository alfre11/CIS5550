package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;

public class FlameRDDImpl implements FlameRDD{
	
	String tableName;
	FlameContextImpl ctx;
	private static final Logger logger = Logger.getLogger(FlameRDDImpl.class);
	
	public FlameRDDImpl(String tableName, FlameContextImpl ctx) {
		this.tableName = tableName;
		this.ctx = ctx;
		
	}

	@Override
	public List<String> collect() throws Exception {
		// TODO Auto-generated method stub
		Iterator<Row> scannedRows = Coordinator.kvs.scan(tableName);
		List<String> ret = new ArrayList<>();
		while(scannedRows.hasNext()) {
			Row curr = scannedRows.next();
			ret.add(curr.get("value"));
		}
		return ret;
	}

	@Override
	public FlameRDD flatMap(StringToIterable lambda) throws Exception {
		// TODO Auto-generated method stub
//		logger.info("flatMap called");
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = ctx.invokeOperation("/rdd/flatMap", serLambda, tableName, null);
		return new FlameRDDImpl(tabName, ctx);
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = ctx.invokeOperation("/rdd/mapToPair", serLambda, tableName, null);
		return new FlamePairRDDImpl(tabName, ctx);
	}

	@Override
	public FlameRDD intersection(FlameRDD r) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlameRDD sample(double f) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FlamePairRDD groupBy(StringToString lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
