package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
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
		String tabName = ctx.invokeOperation("/rdd/flatMap", serLambda, tableName, null, null);
		return new FlameRDDImpl(tabName, ctx);
	}

	@Override
	public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = ctx.invokeOperation("/rdd/mapToPair", serLambda, tableName, null, null);
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

	@Override
	public int count() throws Exception {
		// TODO Auto-generated method stub
		return Coordinator.kvs.count(tableName);
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		// TODO Auto-generated method stub
		KVSClient cli = Coordinator.kvs;
		cli.rename(tableName, tableNameArg);
		this.tableName = tableNameArg;
		
	}

	@Override
	public FlameRDD distinct() throws Exception {
		// TODO Auto-generated method stub
		String tabName = ctx.invokeOperation("/rdd/distinct", null, tableName, null, null);
		return new FlameRDDImpl(tabName, ctx);
	}

	@Override
	public void destroy() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Vector<String> take(int num) throws Exception {
		// TODO Auto-generated method stub
		KVSClient cli = Coordinator.kvs;
		Iterator<Row> iter = cli.scan(tableName);
		int c = 0;
		Vector<String> ret = new Vector<>();
		while(iter.hasNext() && c < num) {
			Row curr = iter.next();
			ret.add(curr.get("value"));
			c++;
		}
		return ret;
	}

	@Override
	public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(lambda);
		String tabName = ctx.invokeOperation("/rdd/flatMapToPair", serLambda, tableName, null, null);
		return new FlamePairRDDImpl(tabName, ctx);
	}

	@Override
	public FlameRDD filter(StringToBoolean lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String fold(String arg0, TwoStringsToString arg1) throws Exception {
		// TODO Auto-generated method stub
		byte[] serLambda = Serializer.objectToByteArray(arg1);
		String tabName = ctx.invokeOperation("/rdd/fold", serLambda, tableName, arg0, null);
		return tabName;
	}

	@Override
	public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
