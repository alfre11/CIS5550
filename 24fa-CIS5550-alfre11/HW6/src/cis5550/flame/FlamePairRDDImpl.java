package cis5550.flame;

import java.util.*;

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
		String tabName = ctx.invokeOperation("/rdd/foldByKey", serLambda, tableName, zeroElement);
		return new FlamePairRDDImpl(tabName, ctx);
	}

}
