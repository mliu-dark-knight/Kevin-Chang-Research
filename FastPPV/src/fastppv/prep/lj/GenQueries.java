package fastppv.prep.lj;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import fastppv.core.QueryProcessor;
import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.util.Config;
import fastppv.util.KeyValuePair;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;

public class GenQueries {

	public static final int numQueries = 1000;

	public static void main(String[] args) throws Exception {
		String procPath = args[0];
//		String frac = args[1]; 
				
		Graph graph = new Graph();
		 graph.loadFromFile(procPath + "graph.node.noAttr", procPath + "graph.edge.noAttr", false);
		List<Node> nList = new ArrayList<Node>(graph.getNodes());
		
		Random rnd = new Random(576298109912L);
		//Set<Node> qList = new HashSet<Node>();
		List<Node[]> qNodes = new ArrayList<Node[]>();
		while (qNodes.size() < numQueries) {
			Node q1 =nList.get(rnd.nextInt(nList.size()));
			Node q2 =nList.get(rnd.nextInt(nList.size()));
			qNodes.add(new Node[]{q1,q2});
			System.out.println(q1.id+"\t"+q2.id);
		}		
		
		
		TextWriter out = new TextWriter(procPath +"/"+ "SPquery.noAttr");
		for (Node[] n : qNodes)
			out.writeln(n[0].id + "\t"+n[1].id);
		out.close();

	}

}
