package fastppv.prep.wikiuser;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import fastppv.data.Graph;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;


public class GenGraph {
	
	private static void addLink(Map<String, Set<String>> linkMap, String from, String to) {
		Set<String> toSet = linkMap.get(from);
		if (toSet == null) {
			toSet = new HashSet<String>();
			linkMap.put(from, toSet);
		}
		
		toSet.add(to);
	}
	
	private static HashMap<String, Integer> genIdMap(Map<String, Set<String>> linkMap) {
		HashMap<String, Integer> idMap = new HashMap<String, Integer>();
		int id = 0;
		for (String x : linkMap.keySet()) {
			if (!idMap.containsKey(x)) {
				idMap.put(x, id);
				id ++;
			}
			for (String y : linkMap.get(x)) {
				if (!idMap.containsKey(y)) {
					idMap.put(y, id);
					id ++;
				}
			}
		}
		
		return idMap;
	}
	
	public static void main(String[] args) throws Exception {
		String rawPath = args[0] + "/";
		String procPath = args[1] + "/";
		int year = Integer.parseInt(args[2]); 
		
		Map<String, Set<String>> linkMap = new HashMap<String, Set<String>>();
		
		TextReader in = new TextReader(rawPath + "enwiki-20120104-usertalk");
		String line;
		while ( (line = in.readln()) != null) {
			String[] split = line.split("\t");
			if (split.length != 3)
				continue;
			String from = split[0];
			String to = split[1];
			int y = Integer.parseInt(split[2].split("-")[0]);
			if (y <= year) {
				addLink(linkMap, from, to);
			}
		}
		in.close();
		
		String tmpNode = procPath + "wikiuser-" + year + "-nodes" + ".tmp";
		String tmpEdge = procPath + "wikiuser-" + year + "-edges" + ".tmp";
		Map<String, Integer> idMap = genIdMap(linkMap);
		TextWriter out = new TextWriter(tmpNode);
		for (String k : idMap.keySet())
			out.writeln(idMap.get(k));
		out.close();
		
		out = new TextWriter(tmpEdge);
		for (String x : linkMap.keySet()) 
			for (String y : linkMap.get(x))
				out.writeln(idMap.get(x) + "\t" + idMap.get(y));
		out.close();

		Graph g = new Graph();
		g.loadFromFile(tmpNode, tmpEdge, false);
		g.preprocess();
		g.saveToFile(procPath + "wikiuser-" + year + "-nodes", procPath + "wikiuser-" + year + "-edges");
		
		(new File(tmpNode)).delete();
		(new File(tmpEdge)).delete();
	}
}
