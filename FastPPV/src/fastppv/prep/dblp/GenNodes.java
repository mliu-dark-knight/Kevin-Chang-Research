package fastppv.prep.dblp;

import java.util.HashSet;
import java.util.Set;

import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;


public class GenNodes {
	
	public static final int AUTHOR_ID_OFFSET = 10000000;
	public static final int VENUE_ID_OFFSET  = 20000000;
	//public static final int TERM_ID_OFFSET   = 30000000;
	
	//public static final int MIN_TERM_COUNT = 5;

	public static void main(String[] args) throws Exception {
		String rawPath = args[0] + "/";
		String procPath = args[1] + "/";
		int year = Integer.parseInt(args[2]);
		String line;
		
		
		System.out.println("Scanning papers...");
		TextReader in = new TextReader(rawPath + "paper.txt");
		HashSet<Integer> paper = new HashSet<Integer>();
		
		while ( (line = in.readln()) != null ) {
			String[] split = line.split("\t");
			int pid = Integer.parseInt(split[0]);
			int y = Integer.parseInt(split[2]);
			if (y <= year)
				paper.add(pid);
		}
		
		in.close();
		
		System.out.println("Scanning authors and venues...");
		in = new TextReader(rawPath + "paperEvent.txt");
		HashSet<Integer> author = new HashSet<Integer>();
		HashSet<Integer> venue = new HashSet<Integer>();
		while ( (line = in.readln()) != null ) {
			String[] split = line.split("\t");
			int pid = Integer.parseInt(split[0]);
			if (!paper.contains(pid))
				continue;
			
			if (!split[1].isEmpty()) {
				int vid = Integer.parseInt(split[1]) + VENUE_ID_OFFSET;
				venue.add(vid);
			}
			
			for (int i = 3; i < split.length; i++) {
				int aid = Integer.parseInt(split[i]) + AUTHOR_ID_OFFSET;
				author.add(aid);
			}
		}
		in.close();

		TextWriter out = new TextWriter(procPath + "dblp-" + year + "-nodes");
		for (int pid : paper)
			out.writeln(pid);
		for (int aid : author)
			out.writeln(aid);
		for (int vid : venue)
			out.writeln(vid);
		//for (int tid : term)
		//	out.writeln(tid);
		out.close();
	}
}
