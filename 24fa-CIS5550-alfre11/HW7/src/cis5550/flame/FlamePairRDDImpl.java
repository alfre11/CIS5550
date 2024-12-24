package cis5550.flame;

import java.util.*;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.*;

public class FlamePairRDDImpl implements FlamePairRDD{
	
	public String tableName;
	FlameContextImpl ctx;
	
	private static final Logger logger = Logger.getLogger(FlamePairRDDImpl.class);
	
	public FlamePairRDDImpl(String tableName, FlameContextImpl ctx) {
		this.tableName = tableName;
		this.ctx = ctx;
	}

	@Override
	public List<FlamePair> collect() throws Exception {
		// TODO Auto-generated method stub
		List<FlamePair> ret = new ArrayList<>();
		Iterator<Row> scannedRows = Coordinator.kvs.scan(this.tableName);
		while(scannedRows.hasNext()) {
			Row curr = scannedRows.next();
			for(String col : curr.columns()) {
				String val = curr.get(col);
				ret.add(new FlamePair(curr.key(), val));
			}
		}
		
		return ret;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = ctx.invokeOperation("/rdd/foldByKey", serLambda, tableName, zeroElement, null);
		return new FlamePairRDDImpl(tabName, ctx);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		// TODO Auto-generated method stub
		KVSClient cli = Coordinator.kvs;
		cli.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = ctx.invokeOperation("/pairrdd/flatMap", serLambda, tableName, null, null);
		return new FlameRDDImpl(tabName, ctx);
	}

	@Override
	public void destroy() throws Exception {
		// TODO Auto-generated method stub
		Coordinator.kvs.delete(tableName);
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = ctx.invokeOperation("/pairrdd/flatMapToPair", serLambda, tableName, null, null);
		return new FlamePairRDDImpl(tabName, ctx);
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) throws Exception {
		// TODO Auto-generated method stub
		String tabName = ctx.invokeOperation("/pairrdd/join", null, tableName, null, (FlamePairRDDImpl) other);
		return new FlamePairRDDImpl(tabName, ctx);
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
