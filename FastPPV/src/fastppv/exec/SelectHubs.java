package fastppv.exec;

import fastppv.core.hubselection.HubSelection;
import fastppv.core.hubselection.InDegHub;
import fastppv.core.hubselection.OutDegHub;
import fastppv.core.hubselection.PageRankHub;
import fastppv.core.hubselection.ProgressiveHub;
import fastppv.core.hubselection.RandomHub;
import fastppv.core.hubselection.UtilityHub;
import fastppv.util.Config;

public class SelectHubs {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		HubSelection h;
		Config.hubType = args[0];
		
		if (args[0].equals("random"))
			h = new RandomHub(Config.nodeFile, Config.edgeFile);
		else if (args[0].equals("pagerank"))
			h = new PageRankHub(Config.nodeFile, Config.edgeFile); 
		else if (args[0].equals("outdeg")) 
			h = new OutDegHub(Config.nodeFile, Config.edgeFile);
		else if (args[0].equals("utility")) {
			Config.hubType += "-" + args[1];
			h = new UtilityHub(Config.nodeFile, Config.edgeFile, Double.parseDouble(args[1]));
		}else if (args[0].equals("progressive")) {
			Config.hubType += "-" + args[1];
			h = new ProgressiveHub(Config.nodeFile, Config.edgeFile, Double.parseDouble(args[1]));
		}else if(args[0].equals("indeg"))
			h= new InDegHub(Config.nodeFile,Config.edgeFile);
		else {
			System.out.println("Unknown hub selection algorithm!");
			return;
		}
		
		h.save();

	}

}
