package cis5550.run;

import cis5550.flame.FlameSubmit;
import cis5550.kvs.KVSClient;

public class RunRanker {

	public static void main(String[] args) throws Exception {
		(new KVSClient("localhost:8000")).delete("pt-finalranks");
		FlameSubmit.submit("localhost:9000", "ranker.jar", "cis5550.jobs.Ranker",
				new String[] { "ability" });
	}

}
