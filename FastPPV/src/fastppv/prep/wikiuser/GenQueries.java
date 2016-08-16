package fastppv.prep.wikiuser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.util.Config;
import fastppv.util.io.TextWriter;

public class GenQueries {

	public static final int numQueries = 1000;

	public static void main(String[] args) throws Exception {
		String procPath =  "C:\\Users\\fanwei.z\\Documents\\fastsim-wiki\\";
	//	String year = args[1]; 
				
		Graph graph = new Graph();
        graph.loadFromFile("C:\\Users\\fanwei.z\\Documents\\fastsim-wiki\\wiki-nodes", "C:\\Users\\fanwei.z\\Documents\\fastsim-wiki\\wiki-edges", false);
		List<Node> nList = new ArrayList<Node>(graph.getNodes());
		
		Random rnd = new Random(576298109912L);
		Set<Node> qList = new HashSet<Node>();
		while (qList.size() < numQueries) {
			qList.add(nList.get(rnd.nextInt(nList.size())));
		}				
		
		TextWriter out = new TextWriter(procPath + "query");
		for (Node n : qList)
			out.writeln(n.id + "");
		out.close();		
	}
}
