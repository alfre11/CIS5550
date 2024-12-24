package cis5550.run;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;

public class RunIndexer {

	public static void main(String[] args) throws Exception {
		(new KVSClient("localhost:8000")).delete("pt-index");
		FlameSubmit.submit("localhost:9000", "indexer.jar", "cis5550.jobs.Indexer",
				new String[] {});
	}

}
