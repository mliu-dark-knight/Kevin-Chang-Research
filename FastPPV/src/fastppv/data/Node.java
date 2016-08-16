package fastppv.data;

import java.util.ArrayList;
import java.util.List;

/**
 * node structure.
 * 
 * @author zfw
 * 
 */
public class Node {

	public int id;
	public boolean isHub = false;
	public boolean inSub = false;
	//public boolean isRead = false;
	//public boolean isDangling = false;
	public int clusterId = -1;
	
	public List<Node> out;
	public List<Node> in;
	
	public List<Node> inInSub;
	
	public List<Integer> outId; 
	public List<Integer> inId;
	
	public double outEdgeWeight = 0;
	public double outEdgeWeightInSub = 0;
	public double vOld = 0;
	public double vNew = 0;

	public Node(int id) {
		this.id = id;
		out = new ArrayList<Node>();
		in = new ArrayList<Node>();
	}
	
	@Override
	public int hashCode() {
		return this.id;
	}
	
	@Override
	public boolean equals(Object o) {
		Node n = (Node)o;
		return n.id == this.id;
	}

	
	public void initOutEdgeWeight() {
		if (out.size() > 0)
			outEdgeWeight = 1.0 / out.size();
		else
			outEdgeWeight = 0;
	}
	
	public void initOutEdgeWeightUsingNeighborId() {
		if (outId.size() > 0)
			outEdgeWeight = 1.0 / outId.size();
		else
			outEdgeWeight = 0;
	}
	
	public void initOutEdgeWeightInSub(int qid) {
		if (this.isHub && this.id != qid) {
			outEdgeWeightInSub = 0;
			return;
		}
		
		int count = 0;
		for (Node n : this.out) {
			if (n.inSub) 
				count++;
		}
		if (count > 0) 
			outEdgeWeightInSub = 1.0 / count;
		else
			outEdgeWeightInSub = 0;
	}
	
}
