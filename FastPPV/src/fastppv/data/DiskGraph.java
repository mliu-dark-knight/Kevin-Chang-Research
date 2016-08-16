package fastppv.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



import fastppv.util.ClusterManager;
import fastppv.util.Config;
import fastppv.util.KeyValuePair;
import fastppv.util.MapCapacity;
import fastppv.util.io.DataReader;


public class DiskGraph extends Graph {
	
	private ClusterManager cm;
	private int clusterFaults;
	private int curClusterId;
	
	public DiskGraph() throws Exception {
		super();
		cm = new ClusterManager();
		clusterFaults = 0;
		curClusterId = -1;
	}
	
	public int numClusterFaults() {
		return clusterFaults - 1;
	}
	
	public void resetClusterFaults() {
		clusterFaults = 0;
	}
	
	private void loadCluster(int nid, Graph sub) {
		clusterFaults ++;
		curClusterId = cm.getClusterId(nid);
		clear();
		
		
		DataReader in;
		try {
			in = new DataReader(ClusterManager.getClusterFile(curClusterId));
		
			int numNodes = in.readInteger();
			nodes = new HashMap<Integer, Node>(MapCapacity.compute(numNodes));
						
			for (int i = 0; i < numNodes; i++) {
				int id = in.readInteger();
				int numOut = in.readInteger();
				int numIn = in.readInteger();
				
				Node n = null;
				boolean notInSub = sub == null || (n = sub.getNode(id)) == null;
				if (notInSub) {
					n = new Node(id);
					if (hubs.contains(n))
						n.isHub = true;
					n.outId = new ArrayList<Integer>(numOut);		
					n.inId = new ArrayList<Integer>(numIn);		
					for (int j = 0; j < numOut; j++) {
						int y = in.readInteger();
						n.outId.add(y);
					}					
					for (int j = 0; j < numIn; j++) {
						int y = in.readInteger();
						n.inId.add(y);
					}
				}
				else {
					for (int j = 0; j < numIn + numOut; j++)
						in.readInteger();
				}
				nodes.put(id, n);

				
			}
			
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		init();
	}
	
	@Override
	public void init() {
		for (Node n : nodes.values())
			n.initOutEdgeWeightUsingNeighborId();
	}
	
	public void reset() {
		clear();
		this.resetClusterFaults();
	}
	
	@Override
	public PrimePPV computePrimePPV(Node q) {
		
		loadCluster(q.id, null);
		q = this.getNode(q.id);
		PrimeSubgraph sub = new PrimeSubgraph(q);
		sub.accumulateReachability(q.id, Config.alpha);

		EntryManager em = new EntryManager(cm);
		recursiveFindNodes(sub, q, em);
		
		KeyValuePair e;
		while ( (e = em.extract(curClusterId)) != null) {
			if (cm.getClusterId(e.key) != curClusterId) {
				if (Config.maxClusterFaults != -1 && this.numClusterFaults() >= Config.maxClusterFaults)
					break;
				loadCluster(e.key, sub);
			}
			Node n = this.getNode(e.key);
			if (!sub.containsNode(n.id))
				sub.addNode(n);
			
			if (!n.isHub && e.value >= Config.epsilon)
				recursiveFindNodes(sub, n, em);
		}        
      
        for (Node x : sub.getNodes()) {
        	for (int id : x.outId) {
        		Node n = sub.getNode(id);
        		if (n != null)
        			x.out.add(n);
        	}
        	for (int id : x.inId) {
        		Node n = sub.getNode(id);
        		if (n != null)
        			x.in.add(n);
        	}
   		}
        
        sub.init();     
        sub.computePrimePPV();
        sub.reset();
        return sub.toPrimePPV();
	}
	
	protected void recursiveFindNodes(PrimeSubgraph sub, Node n, EntryManager em) {
		double cur = sub.getReachability(n.id);
	    for (int mid : n.outId) {
	    	double next = cur * (1 - Config.alpha) * n.outEdgeWeight;
            sub.accumulateReachability(mid, next);
	    	if (sub.containsNode(mid))
	    		continue;
	    	
            Node m = this.getNode(mid);
	    	if (m == null) {
	    		em.add(mid, next);
	    		continue;
	    	}
	        
	    	sub.addNode(m);
	    	if (m.isHub || next < Config.epsilon)
	        	continue;
	        
	        recursiveFindNodes(sub, m, em);
	     }
	}
}

class EntryManager {
	
	private Map<Integer, Double>[] maps;
	private ClusterManager cm;
	
	
	@SuppressWarnings("unchecked")
	public EntryManager(ClusterManager cm) {
		this.cm = cm;
		
		maps = new HashMap[Config.numClusters];
		for (int i = 0; i < Config.numClusters; i++)
			maps[i] = new HashMap<Integer, Double>();
	}
	
	private KeyValuePair extractOneEntry(int clusterId) {
		Iterator<Map.Entry<Integer, Double>> iter = maps[clusterId].entrySet().iterator();
		Map.Entry<Integer, Double> e = iter.next();
		iter.remove();
		return new KeyValuePair(e.getKey(), e.getValue());
	}
	
	private double getTotalReachability(int clusterId) {
		double sum = 0;
		for (double v : maps[clusterId].values())
			sum += v;
		return sum;
	}
	
	public KeyValuePair extract(int clusterId) {
		
		if (!maps[clusterId].isEmpty())
			return extractOneEntry(clusterId);
		
		double max = -1;
		int maxI = -1;
		for (int i = 0; i < Config.numClusters; i++) {
			double sum = getTotalReachability(i);
			if (sum > max) {
				max = sum;
				maxI = i;
			}
		}
		
		if (max > 0)
			return extractOneEntry(maxI);
		else
			return null;
	}
	
	public void add(int id, double reachability) {
		int clusterId = cm.getClusterId(id);
		Double v = maps[clusterId].get(id);
		if (v == null)
			maps[clusterId].put(id, reachability);
		else
			if (reachability > v)
				maps[clusterId].put(id, reachability);
	}
	
	
}
