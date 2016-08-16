package fastppv.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import fastppv.util.ClusterManager;
import fastppv.util.Config;
import fastppv.util.IndexManager;
import fastppv.util.KeyValuePair;
import fastppv.util.io.DataWriter;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;

public class Graph {

    protected Map<Integer, Node> nodes;
    protected Set<Node> hubs;
    private Random rnd;
    private int nodesInsub;
    private double edgesInsub;

    // private static final int NUM_DUMMY_EDGES = 1;

    public Graph() {
        nodes = new HashMap<Integer, Node>();
        rnd = new Random(9876804367L);
        nodesInsub =0;
        edgesInsub =0.0;
    }

    public void preprocess() {
        Set<Node> remove = new HashSet<Node>();
        for (Node n : nodes.values()) {
            // if (n.in.size() < minNumEdges || n.out.size() < minNumEdges) {
            if (n.in.isEmpty() || n.out.isEmpty())
                remove.add(n);
        }
        if (remove.isEmpty())
            return; // finished

        System.out.println(remove.size() + " nodes to be removed...");

        for (Node n : remove) {
            // remove from out neighbors' in
            for (Node m : n.out)
                m.in.remove(n);

            // remove from in neighbors' out
            for (Node m : n.in)
                m.out.remove(n);

            nodes.remove(n.id);
        }

        preprocess();
    }

    public void saveToFile(String nodeFile, String edgeFile) throws Exception {
        TextWriter out = new TextWriter(nodeFile);
        for (Node n : nodes.values())
            out.writeln(n.id);
        out.close();

        out = new TextWriter(edgeFile);
        int count = 0;
        for (Node n : nodes.values())
            for (Node m : n.out) {
                out.writeln(n.id + "\t" + m.id);
                count++;
            }
        out.close();

        System.out.println("# Nodes: " + nodes.size());
        System.out.println("# Edges: " + count);
    }

    public void clear() {
        nodes.clear();
    }


    private void loadGraphFromFile(String nodeFile, String edgeFile) throws Exception {
        clear();

        TextReader inN = new TextReader(nodeFile);
        TextReader inE = new TextReader(edgeFile);
        String line;

        System.out.print("Loading graph");
        int count = 0;
        while ((line = inN.readln()) != null) {
            int id = Integer.parseInt(line);
            this.addNode(new Node(id));

            count++;
            if (count % 1000000 == 0)
                System.out.print(".");
        }

        while ((line = inE.readln()) != null) {
            String[] split = line.split("\t");
            int from = Integer.parseInt(split[0]);
            int to = Integer.parseInt(split[1]);
            this.addEdge(from, to);

            count++;
            if (count % 1000000 == 0)
                System.out.print(".");
        }
        System.out.println();

        inN.close();
        inE.close();

        init();
    }

    public void loadFromFile(String nodeFile, String edgeFile,
                             boolean identifyHubs) throws Exception {
        loadGraphFromFile(nodeFile, edgeFile);

        if (identifyHubs){
           String hubNodeFile= IndexManager.getHubNodeFile();
           loadHubs(hubNodeFile);
        }

    }

    public void loadFromFile(String nodeFile, String edgeFile,
                             String hubFile) throws Exception {
        loadGraphFromFile(nodeFile, edgeFile);

        loadHubs(hubFile);

    }

    public void loadHubs(String hubNodeFile) throws Exception {
        TextReader in = new TextReader(hubNodeFile);
        String line;

        hubs = new HashSet<Node>();
        while ((line = in.readln()) != null) {
            int id = Integer.parseInt(line);
            Node n = getNode(id);
            if (n == null)
                n = new Node(id);
            n.isHub = true;
            hubs.add(n);
            if (hubs.size() == Config.numHubs)
                break;
        }

        in.close();
    }

    public Set<Node> getHubs() {
        return hubs;
    }

    public void addNode(Node n) {
        nodes.put(n.id, n);
    }

    public void addEdge(int from, int to) {
        Node nFrom = getNode(from);
        Node nTo = getNode(to);
        nFrom.out.add(nTo);
        nTo.in.add(nFrom);
    }

    public PrimePPV computePrimePPV(Node q) {
        // System.out.print("x");
        PrimeSubgraph sub = new PrimeSubgraph(q);
        sub.accumulateReachability(q.id, Config.alpha);
        recursiveFindNodes(sub, q);

        // System.out.print("y");
        sub.init();
        sub.computePrimePPV();
        // System.out.print("z");
      
        // for counting the number of nodes/edges in each prime subgraph
        /*for(Node n: nodes.values())
        	if (n.inSub){
        		nodesInsub ++;
        		for(Node m: n.out)
        			if (m.inSub)
        				edgesInsub++;
        	}
        System.out.println(nodesInsub + " "+ edgesInsub);	*/
        
        
        sub.reset();
        return sub.toPrimePPV();
    }
    
    public PrimeSubgraph computePrimeSubPPV(Node q) {
        // System.out.print("x");
        PrimeSubgraph sub = new PrimeSubgraph(q);
        sub.accumulateReachability(q.id, Config.alpha);
        recursiveFindNodes(sub, q);

        // System.out.print("y");
        sub.init();
     //   sub.computePrimePPV();
        // System.out.print("z");
        
        sub.reset();
        
        return sub;
    }

    protected void recursiveFindNodes(PrimeSubgraph sub, Node n) {
        double cur = sub.getReachability(n.id);
        for (Node m : n.out) {
            double next = cur * (1 - Config.alpha) * n.outEdgeWeight;
            sub.accumulateReachability(m.id, next);
            if (m.inSub)
                continue;
            sub.addNode(m);
            if (m.isHub || next < Config.epsilon)
                continue;

            recursiveFindNodes(sub, m);
        }
    }

    public int numNodes() {
        return nodes.size();
    }

    public Node getNode(int id) {
        return nodes.get(id);
    }

    public Collection<Node> getNodes() {
        return nodes.values();
    }

    public boolean containsNode(int id) {
        return nodes.containsKey(id);
    }

    public void computePageRank(int q) {
        double d = 1.0 / numNodes();
        for (Node n : nodes.values()) {
            if (q == -1)
                n.vOld = d;
            else
                n.vOld = (n.id == q) ? 1.0 : 0;
        }

        for (int i = 0; i < Config.numIterations; i++) {
            for (Node n : nodes.values()) {
                n.vNew = 0;
                for (Node m : n.in)
                    n.vNew += m.outEdgeWeight * m.vOld;

                double delta;
                if (q == -1)
                    delta = d;
                else
                    delta = (n.id == q) ? 1.0 : 0;

                n.vNew = n.vNew * (1 - Config.alpha) + delta * Config.alpha;
            }

            for (Node n : nodes.values()) {
                n.vOld = n.vNew;
            }
        }
    }

   /* public void computeMultiPPV(List<KeyValuePair> hubs) {
        Map<Integer, Double> map = convertPairToMap(hubs);
        double initWeight = 1.0 / hubs.size();
        for (Node n : nodes.values()) {
            n.vOld = 0;
            if (map.get(n.id) != null) {
                n.vOld = initWeight;
            }
        }

        for (int i = 0; i < Config.numIterations; i++) {
            for (Node n : nodes.values()) {
                n.vNew = 0;
                for (Node m : n.in)
                    n.vNew += m.outEdgeWeight * m.vOld;
                double delta;
                delta = (map.get(n.id) != null) ? initWeight : 0;

                n.vNew = n.vNew * (1 - Config.alpha) + delta * Config.alpha;
            }

            for (Node n : nodes.values()) {
                n.vOld = n.vNew;
            }
        }
    }*/
    public void computeMultiPPV(List<Node> hubs) {
       
        double initWeight = 1.0 / hubs.size();
        for (Node n : nodes.values()) {
        	if (hubs.contains(n))
        		n.vOld=initWeight;
        	else
        		n.vOld = 0;
        }

        for (int i = 0; i < Config.numIterations; i++) {
            for (Node n : nodes.values()) {
                n.vNew = 0;
                for (Node m : n.in)
                    n.vNew += m.outEdgeWeight * m.vOld;
                double delta;
                if(hubs.contains(n))
                	delta = initWeight;
                else
                	delta = 0;
               

                n.vNew = n.vNew * (1 - Config.alpha) + delta * Config.alpha;
            }

            for (Node n : nodes.values()) {
                n.vOld = n.vNew;
            }
        }
    }

    private Map<Integer, Double> convertPairToMap(List<KeyValuePair> hubs2) {
        Map<Integer, Double> map = new HashMap<Integer, Double>(hubs2.size());
        for (KeyValuePair pair : hubs2) {
            map.put(pair.key, pair.value);
        }
        return map;
    }

    public void init() {
        for (Node n : nodes.values())
            n.initOutEdgeWeight();
    }

    public PPV toPPV() {
        PPV ppv = new PPV(numNodes());
        for (Node n : nodes.values())
            ppv.set(n.id, n.vNew);

        return ppv;
    }

	/*
     * private Node[] selectAnchors() throws Exception { List<Node> nList = new
	 * ArrayList<Node>(nodes.values()); Random rnd = new Random(9876804367L);
	 * HashSet<Node> anchors = new HashSet<Node>(); while (anchors.size() <
	 * Config.numClusters) anchors.add(nList.get(rnd.nextInt(nList.size())));
	 * 
	 * Node[] result = new Node[Config.numClusters]; return
	 * anchors.toArray(result); }
	 * 
	 * 
	 * public void cluster() throws Exception {
	 * System.out.println("Selecting anchors..."); Node[] anchor =
	 * selectAnchors(); PPV[] ppv = new PPV[Config.numClusters];
	 * 
	 * System.out.print("Computing PPV for anchor "); for (int i = 0; i <
	 * Config.numClusters; i++) { System.out.print(i + " ");
	 * this.computePageRank(anchor[i].id); ppv[i] = this.toPPV(); }
	 * System.out.println();
	 * 
	 * @SuppressWarnings("unchecked") HashSet<Node>[] cluster = new
	 * HashSet[Config.numClusters]; for (int i = 0; i < anchor.length; i++)
	 * cluster[i] = new HashSet<Node>();
	 * 
	 * int sizeCap = (int)(nodes.size() * Config.clusterSizeCapRatio /
	 * Config.numClusters); for (Node n : nodes.values()) { double max = -1; int
	 * maxI = -1; for (int i = 0; i < Config.numClusters; i++) { if
	 * (cluster[i].size() >= sizeCap) continue; double score = ppv[i].get(n.id);
	 * if (score > max) { max = score; maxI = i; } } cluster[maxI].add(n); }
	 * 
	 * // output stats System.out.println("# nodes in each cluster:"); for
	 * (HashSet<Node> c : cluster) System.out.println(c.size());
	 * 
	 * // writing mapping TextWriter out = new
	 * TextWriter(ClusterManager.getClusterMappingFile()); for (int i = 0; i <
	 * Config.numClusters; i++) { for (Node n : cluster[i]) out.writeln(n.id +
	 * "\t" + i); } out.close();
	 * 
	 * // writing clusters for (int i = 0; i < Config.numClusters; i++) {
	 * DataWriter dout = new DataWriter(ClusterManager.getClusterFile(i));
	 * dout.writeInteger(cluster[i].size()); for (Node n : cluster[i]) {
	 * dout.writeInteger(n.id); dout.writeInteger(n.out.size());
	 * dout.writeInteger(n.in.size()); for (Node m : n.out)
	 * dout.writeInteger(m.id); for (Node m : n.in) dout.writeInteger(m.id); }
	 * dout.close(); } }
	 */

    private void splitCluster(int oldId, int newId) {
        List<Node> l = new ArrayList<Node>();
        for (Node n : nodes.values())
            if (n.clusterId == oldId)
                l.add(n);

        Node x = l.get(rnd.nextInt(l.size()));
        Node y = null;
        do {
            y = l.get(rnd.nextInt(l.size()));
        }
        while (y.id == x.id);

        this.computePageRank(x.id);
        PPV px = this.toPPV();

        this.computePageRank(y.id);
        PPV py = this.toPPV();

        for (Node n : l) {
            double vx = px.get(n.id);
            double vy = py.get(n.id);
            if (vx > vy)
                n.clusterId = newId;
            else if (vx == vy) {
                if (rnd.nextBoolean())
                    n.clusterId = newId;
            }
        }
    }

    private int findTargetClusterId() {
        Map<Integer, Integer> m = new HashMap<Integer, Integer>();
        for (Node n : nodes.values()) {
            Integer count = m.get(n.clusterId);
            if (count == null)
                count = 0;
            m.put(n.clusterId, count + 1);
        }

        int maxI = -1;
        int max = -1;
        for (Map.Entry<Integer, Integer> e : m.entrySet()) {
            if (e.getValue() > max) {
                max = e.getValue();
                maxI = e.getKey();
            }
        }

        return maxI;
    }

    @SuppressWarnings("unchecked")
	public void kmeans(int k) throws Exception{
    	
    	 List<Node>[] cluster = new ArrayList[k];
    	 
         for (int i = 0; i < k; i++)
             cluster[i] = new ArrayList<Node>();

         
    	for (Node n : nodes.values())
            n.clusterId = -1;
    	
    	//randomly choose k centroids
/*    	List<Node>existCentroids = new ArrayList();
    	for (int i =0; i<k; i++){
    		boolean existed = false;
    		
    		do{
    			
    		Node x = nodes.get(rnd.nextInt(nodes.size()));
    		
    		if(existCentroids == null)
    		{
    			x.clusterId=i;
    			cluster[i].add(x);
    			existCentroids.add(x);
    			continue;    			
    			
    		}
    		
    		existed = false;
    		for (Node n: existCentroids){
    					
    				if (x.id == n.id)
    					existed = true;
    			}
    		}
    		while(existed);
    		
    		x.clusterId=i;
    		cluster[i].add(x);
    		System.out.println("finding a centroid: "+x.id + " clusterId is: " + x.clusterId);    		
      }*/
      
    	List<Node> existList = new ArrayList();
    	for (int i= 0; i < k; i++) {
    		Node x = nodes.get(rnd.nextInt(nodes.size()));
    		System.out.println(x.id);
    		if (x !=null){ 
    			existList.add(x);
    			x.clusterId = i;
    			cluster[i].add(x);
    		}			
		}
		for (int i =0; i<cluster.length;i++){
    		System.out.println("\nCluster " +i);
    		for (int j =0; j <cluster[i].size();j++)
    			System.out.print(cluster[i].get(j).id + " ");
    	   	
    	}
		
    	
    	//assign nodes to clusters
    	int iterations = 0;
    	while(iterations < 10 && true){
    		for (Node n: nodes.values()){
    			System.out.println("Assign node: " + n.id);
        		double max =0.0;
        		int oldCid = n.clusterId;
        		int newCid = oldCid;
        		for (int i = 0; i < cluster.length;i++){
        		//	System.out.println("Running...");
            		List<Node> centroids = cluster[i];
            		
            		this.computeMultiPPV(centroids);
            		PPV rc = this.toPPV();
            		double simTocentroid = rc.get(n.id);
          //  		System.out.println("ppv value: "+ simTocentroid);
            		if (simTocentroid >= max){
       				 max = simTocentroid;
       				 newCid = i;
       				}   
            //		 System.out.println("New cluster id: " +newCid);
       			
       				 
//       			 for (int ii = 0; ii < cluster[oldCid].size(); ii++)
//       			 System.out.println(cluster[oldCid].get(ii).id);
       			 if (oldCid != -1)
       				 cluster[oldCid].remove(n);
       		  	
       			 cluster[newCid].add(n);
       			 n.clusterId=newCid; 
            	}   		   		
        	}
    		System.out.println("Line 504: clusters in iteration " + iterations );
    		for (int i =0; i<cluster.length;i++){
        		System.out.println("Cluster " +i);
        		
        		for (int j =0; j <cluster[i].size();j++)
        			System.out.print(cluster[i].get(j).id + " ");
        		System.out.println();
        	   	
        	}
    		
    		iterations++;
    	}
    	//print out clusters
    	
    		
    }
    
    public void cluster() throws Exception {

        for (Node n : nodes.values())
            n.clusterId = 0;

        System.out.print("Pass");
        for (int i = 1; i < Config.numClusters; i++) {
            System.out.print(" " + i);
            int target = findTargetClusterId();
            splitCluster(target, i);
        }
        System.out.println();

        @SuppressWarnings("unchecked")
        HashSet<Node>[] cluster = new HashSet[Config.numClusters];
        for (int i = 0; i < Config.numClusters; i++)
            cluster[i] = new HashSet<Node>();

        for (Node n : nodes.values()) {
            cluster[n.clusterId].add(n);
        }

        // output stats
        System.out.println("# nodes in each cluster:");
        for (int i = 0; i < Config.numClusters; i++)
            System.out.println(cluster[i].size());
        //	for (HashSet<Node> c : cluster)
        //	System.out.println("\t" + c.size());

        // writing mapping
        TextWriter out = new TextWriter(ClusterManager.getClusterMappingFile());
        for (int i = 0; i < Config.numClusters; i++) {
            for (Node n : cluster[i])
                out.writeln(n.id + "\t" + i);
        }
        out.close();

        // writing clusters
		/*for (int i = 0; i < Config.numClusters; i++) {
			DataWriter dout = new DataWriter(ClusterManager.getClusterFile(i));
			dout.writeInteger(cluster[i].size());
			for (Node n : cluster[i]) {
				dout.writeInteger(n.id);
				dout.writeInteger(n.out.size());
				dout.writeInteger(n.in.size());
				for (Node m : n.out)
					dout.writeInteger(m.id);
				for (Node m : n.in)
					dout.writeInteger(m.id);
			}
			dout.close();
		}*/

        //writing clustered graphs
        for (int i = 0; i < Config.numClusters; i++) {
            TextWriter eout = new TextWriter(ClusterManager.getClusterEdge(i));
            TextWriter nout = new TextWriter(ClusterManager.getClusterNode(i));

            for (Node n : cluster[i]) {
                nout.writeln(n.id);
                for (Node m : n.out)
                    if (cluster[i].contains(m))
                        eout.writeln(n.id + "\t" + m.id);
            }
            eout.close();
            nout.close();
        }
        System.out.println("Clustering end.");
    }

}
