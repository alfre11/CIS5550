package cis5550.run;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;

public class RunPageRank {

	public static void main(String[] args) throws Exception {
		(new KVSClient("localhost:8000")).delete("pt-pageranks");
		FlameSubmit.submit("localhost:9000", "pagerank.jar", "cis5550.jobs.PageRank",
				new String[] { "0.001" });
	}

}
