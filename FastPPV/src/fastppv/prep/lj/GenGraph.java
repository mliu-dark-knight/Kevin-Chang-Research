package fastppv.prep.lj;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import fastppv.data.Graph;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;


public class GenGraph {
	
	
	
	public static void main(String[] args) throws Exception {
		
		String rawPath = args[0] + "\\";
		String procPath = args[1] + "\\";
		String frac = args[2]; 
		
		Set<Integer> nodes = new HashSet<Integer>();
		
		TextReader in = new TextReader(rawPath + "lj-edges_"+frac+"_sample");
		String line;
		while ( (line = in.readln()) != null) {
			if (line.startsWith("#"))
				continue;
			String[] split = line.split("\t");
			int from = Integer.parseInt(split[0]);
			int to = Integer.parseInt(split[1]);
			nodes.add(from);
			nodes.add(to);
		}
		in.close();
		
		String nodeTmp = procPath + "lj-" + frac + "-nodes.tmp";
		TextWriter out = new TextWriter(nodeTmp);
		for (int n : nodes)
			out.writeln(n);
		out.close();
		
		String edgeTmp = procPath + "lj-" + frac + "-edges.tmp";
		out = new TextWriter(edgeTmp);
		in = new TextReader(rawPath + "lj-edges_"+frac+"_sample");
		while ( (line = in.readln()) != null) {
			if (!line.startsWith("#"))
				out.writeln(line);
		}
		in.close();
		out.close();
		
		Graph g = new Graph();
		g.loadFromFile(nodeTmp, edgeTmp, false);
		g.preprocess();
		g.saveToFile(procPath + "lj-" + frac + "-nodes", procPath + "lj-" + frac + "-edges");
		
		( new File(nodeTmp) ).delete();
		( new File(edgeTmp) ).delete();
		
	}
}
