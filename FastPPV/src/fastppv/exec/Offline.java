package fastppv.exec;

import fastppv.core.Indexer;
import fastppv.data.Graph;
import fastppv.util.Config;
import fastppv.util.IndexManager;

public class Offline {
	
	public static void main(String[] args) throws Exception {
	    Config.hubType = args[0];
		Config.numHubs = Integer.parseInt(args[1]);
		boolean forceUpdate = args[2].equals("1");
		
	    Graph g = new Graph();
	    g.loadFromFile(Config.nodeFile, Config.edgeFile, true);
	    
	    IndexManager.mkSubDirDeep();
	    Indexer.index(g.getHubs(), g, forceUpdate);
	}
}
