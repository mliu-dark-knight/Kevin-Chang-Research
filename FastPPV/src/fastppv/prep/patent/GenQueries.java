package fastppv.prep.patent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.util.io.TextWriter;

public class GenQueries {

	public static final int numQueries = 1000;

	public static void main(String[] args) throws Exception {
		String procPath = args[0] + "/";
		String fileName = args[1]; 
				
		Graph graph = new Graph();
        graph.loadFromFile(procPath + fileName  + "nodes", procPath + fileName + "edges", false);
		List<Node> nList = new ArrayList<Node>(graph.getNodes());
		//single source
		Random rnd = new Random(576298109912L);
		Set<Node> qList = new HashSet<Node>();
		while (qList.size() < numQueries) {
			qList.add(nList.get(rnd.nextInt(nList.size())));
		}		
		
		
		TextWriter out = new TextWriter(procPath + fileName  + "-SSquery");
		for (Node n : qList)
			out.writeln(n.id + "");
		out.close();
		
		
		Random rnd2 = new Random(576298109912L);
		//Set<Node> qList = new HashSet<Node>();
		List<Node[]> qNodes = new ArrayList<Node[]>();
		while (qNodes.size() < numQueries) {
			Node q1 =nList.get(rnd2.nextInt(nList.size()));
			Node q2 =nList.get(rnd2.nextInt(nList.size()));
			qNodes.add(new Node[]{q1,q2});
			//System.out.println(q1.id+"\t"+q2.id);
		}		
		
		
		TextWriter out2 = new TextWriter(procPath +fileName +"SPquery");
		for (Node[] n : qNodes)
			out2.writeln(n[0].id + "\t"+n[1].id);
		out2.close();
		
		
	}

}
