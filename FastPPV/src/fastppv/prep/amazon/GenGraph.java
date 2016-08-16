package fastppv.prep.amazon;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fastppv.data.Graph;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;


public class GenGraph {
	
	
	public static void main(String[] args) throws Exception {
		String rawPath = args[0] + "/";
		String procPath = args[1] + "/";
		double frac = Double.parseDouble(args[2]); 
		
		Set<Integer> nodes = new HashSet<Integer>();
		
		TextReader in = new TextReader(rawPath + "amazon0601.txt");
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
		
		List<Integer> nList = new ArrayList<Integer>(nodes);
		Collections.sort(nList);
		nList = nList.subList(0, (int)(nList.size() * frac));
		
		TextWriter out = new TextWriter(procPath + "amazon-" + frac + "-nodes");
		for (int n : nList)
			out.writeln(n);
		out.close();
		
		out = new TextWriter(procPath + "amazon-" + frac + "-edges");
		in = new TextReader(rawPath + "amazon0601.txt");
		while ( (line = in.readln()) != null) {
			if (line.startsWith("#"))
				continue;
			String[] split = line.split("\t");
			int from = Integer.parseInt(split[0]);
			int to = Integer.parseInt(split[1]);
			if (nodes.contains(from) && nodes.contains(to))
				out.writeln(from + "\t" + to);
		}
		in.close();
		out.close();
		
	}
}
