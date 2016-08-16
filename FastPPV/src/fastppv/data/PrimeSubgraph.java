package fastppv.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import fastppv.util.Config;


public class PrimeSubgraph extends Graph {
	private Map<Integer, Double> reachability = new HashMap<Integer, Double>();
	private Node q;
	
	private final double ITER_STOP = 0.001;
	
	public void accumulateReachability(int nid, Double value) {
		reachability.put(nid, getReachability(nid) + value);
	}
	
	public PrimeSubgraph(Node q) {
		super();
		this.q = q;
		addNode(q);
	}		
	
	@Override
	public void addNode(Node n) {
		super.addNode(n);
		n.inSub = true;
	}

	public double getReachability(int nid) {
		Double d = reachability.get(nid);
		if (d == null)
			return 0;
		else 
			return d;
	}

	public void reset() {
		for (Node n : nodes.values()) {
			n.inSub = false;
			n.inInSub = null;
			//n.isDangling = false;
		}
	}
	
	public void computePrimePPV() {
		//System.out.println(nodes.size());
		for (Node n : nodes.values())
			n.vOld = (n.id == q.id) ? 1.0 : 0;
		
		for (int i = 0; i < Config.numIterations; i++) {
			double sum = 0;
			
			for (Node n : nodes.values()) {
				n.vNew = 0;
				//for (Node m : n.in)
				//	if (m.inSub)
				for (Node m : n.inInSub)
					n.vNew += m.outEdgeWeightInSub * m.vOld;
				if (n.isHub && n.id != q.id)
					n.vNew += n.vOld;
				
				n.vNew *= (1 - Config.alpha);	
			}
			q.vNew += Config.alpha;
			
			for (Node n : nodes.values()) {
				sum += Math.abs(n.vNew - n.vOld);
				n.vOld = n.vNew;
			}
			
			if (sum < ITER_STOP)
				break;
			//System.out.println(sum);
			
		}
	}
	
	public PrimePPV toPrimePPV() {
		PrimePPV ppv = new PrimePPV();
		
		ppv.set(q, q.vNew); // query node must be set first
		for (Node n : nodes.values()) {
			if (n.id != q.id)
				ppv.set(n, n.vNew);
		}

		return ppv;
	}

	@Override
	public void init() {
		for (Node n : nodes.values()) {
			n.initOutEdgeWeightInSub(q.id);
			
			n.inInSub = new ArrayList<Node>();
			for (Node m : n.in)
				if (m.inSub)
					n.inInSub.add(m);
					
		}		
	}
	

}
