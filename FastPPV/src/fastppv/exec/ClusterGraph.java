package fastppv.exec;


import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.util.ClusterManager;
import fastppv.util.Config;


public class ClusterGraph {

    public static void main(String[] args) throws Exception {
    	//Config.numClusters = Integer.parseInt(args[0]);
    	int k = Integer.parseInt(args[0]);
    	
    	System.out.println("Loading graph...");
        Graph graph = new Graph();
        graph.loadFromFile(Config.nodeFile, Config.edgeFile, false);
      for (Node n:  graph.getNodes())
    	  System.out.println(n.in.size());
        
        System.out.println("Clustering graph...");
        ClusterManager.mkSubDir();
       // graph.cluster();
        graph.kmeans(k);
    }

}
