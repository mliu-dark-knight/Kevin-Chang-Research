package fastppv.core.hubselection;

import java.util.ArrayList;
import java.util.Map.Entry;

import fastppv.data.PPV;
import fastppv.util.KeyValuePair;


public class UtilityHub extends HubSelection {

	protected double pagerankPow;
	protected double outDegPow;
	
	public UtilityHub(String nodeFile, String edgeFile, double outDegPow) throws Exception {
		super(nodeFile, edgeFile);
		this.pagerankPow = 1.0;
		this.outDegPow = outDegPow;
	}

	@Override
	protected void fillNodes() {
		nodes = new ArrayList<KeyValuePair>();
		
		graph.computePageRank(-1);
		PPV ppv = graph.toPPV();
		for (Entry<Integer, Double> e : ppv.getEntrySet()) {
			int id = e.getKey();
			double utility = Math.pow(e.getValue(), pagerankPow) * Math.pow(graph.getNode(id).out.size(), outDegPow);
			nodes.add(new KeyValuePair(id, utility));
		}
	}
	

}
