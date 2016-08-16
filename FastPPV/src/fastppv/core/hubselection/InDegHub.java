package fastppv.core.hubselection;

import java.util.ArrayList;
import java.util.Random;
import java.util.Map.Entry;

import fastppv.data.Node;
import fastppv.data.PPV;
import fastppv.util.KeyValuePair;


public class InDegHub extends HubSelection {

	protected double pagerankPow;
	protected double inDegPow;
	
	public InDegHub(String nodeFile, String edgeFile) throws Exception {
		super(nodeFile, edgeFile);
		
	}

	@Override

	
	protected void fillNodes() {
		nodes = new ArrayList<KeyValuePair>();
		
		for (Node n : graph.getNodes())
			nodes.add(new KeyValuePair(n.id, n.in.size()));
	}
	

}
