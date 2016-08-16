package fastppv.core;

import java.io.File;
import java.util.Set;

import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.data.PrimePPV;
import fastppv.data.PrimeSubgraph;
import fastppv.util.Config;
import fastppv.util.IndexManager;
import fastppv.util.io.TextWriter;

public class Indexer {

	public static void index(Set<Node> hubs, Graph graph, boolean forceUpdate)
			throws Exception {

		long storage = 0;

		long start = System.currentTimeMillis();
		int totalHubs =0;
		int count = 0;
		StringBuilder sb = new StringBuilder();
		
		for (Node h : hubs) {
		//	System.out.println("hub: "+ h.id);
			if (forceUpdate
					|| !(new File(IndexManager.getPrimePPVFilename(h.id)))
							.exists()) {
				count++;
				if (count % 100 == 0)
					System.out.print("+");

				PrimePPV ppv = graph.computePrimePPV(h);
				sb.append("HubID: "+ h.id+" "+ppv.getCountInfo());
				ppv.trim(Config.clip);
				sb.append(" "+ppv.getCountInfo()+" \n ");
				storage += ppv.computeStorageInBytes();
				totalHubs += ppv.getHubSize();
				ppv.saveToDisk(false);
				
				
			}
		}
		
		System.out.println("Total border hubs in all building blocks: "+ totalHubs);
		

		TextWriter countWriter=new TextWriter(IndexManager.getIndexPPVCountInfoFilename());
		countWriter.write(sb.toString());
		countWriter.close();
		
		long time = System.currentTimeMillis() - start;

		TextWriter out = new TextWriter(IndexManager.getIndexDeepDir()
				+ "stats.txt");
		out.writeln("Space (mb): " + (storage / 1024.0 / 1024.0));
		out.writeln("Time (hr): " + (time / 1000.0 / 3600.0));
		out.close();
	}
}
