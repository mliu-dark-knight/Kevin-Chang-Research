package fastppv.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.util.Config;
import fastppv.util.IndexManager;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;

public class CommunityFinder {

	public Map<String, List<Edge>> findCommunity(String clusterMapFile)
			throws Exception {
		Graph graph = new Graph();
		graph.loadFromFile(Config.nodeFile, Config.edgeFile, false);

		return findCommunity(graph, parseNodeClusterMap(clusterMapFile));

	}

	public Map<String, String> parseNodeClusterMap(String clusterMapFile)
			throws Exception {
		Map<String, String> nodeClusterMap = new HashMap<String, String>();
		TextReader inN = new TextReader(clusterMapFile);
		String line;

		while ((line = inN.readln()) != null) {
			String s[] = line.split("\t");
			if (s == null || s.length != 2) {
				System.err.println("error node cluster map line:" + line);
				continue;
			}
			nodeClusterMap.put(s[0], s[1]);
		}
		return nodeClusterMap;
	}

	private Map<String, List<Edge>> findCommunity(Graph g,
			Map<String, String> nodeClusterMap) {
		Map<String, List<Edge>> communities = new HashMap<String, List<Edge>>();
		for (Map.Entry<String, String> e : nodeClusterMap.entrySet()) {
			String nodeid = e.getKey();
			String cluster = e.getValue();
			if (cluster == null || cluster.trim().equals("")) {
				continue;
			}
			Node n = g.getNode(Integer.valueOf(nodeid));
			if (n == null) {
				System.err.println("node not found: " + nodeid);
				continue;
			}
			for (Node neighbor : n.out) {
				if (cluster.equals(nodeClusterMap.get(neighbor.id + ""))) {
					if (communities.get(cluster) != null) {
						communities.get(cluster).add(
								new Edge(n.id + "", neighbor.id + ""));
					} else {
						List<Edge> edges = new ArrayList<Edge>();
						edges.add(new Edge(n.id + "", neighbor.id + ""));
						communities.put(cluster, edges);
					}
				}
			}
		}
		return communities;
	}

	class Edge {
		String from;
		String to;

		public Edge(String from, String to) {
			this.from = from;
			this.to = to;
		}

		public String toString() {
			return from + "\t" + to;
		}
	}

	public void saveCommunity(Map<String, List<Edge>> communities,
			String outputDir) throws Exception {

		if (!outputDir.endsWith(IndexManager.FILE_SEP))
			outputDir += IndexManager.FILE_SEP;
		for (Map.Entry<String, List<Edge>> e : communities.entrySet()) {
			if (e.getValue() == null || e.getValue().isEmpty()) {
				continue;
			}
			TextWriter countWriter = new TextWriter(outputDir + e.getKey());
			for (Edge edge : e.getValue()) {
				countWriter.writeln(edge.toString());
			}

			countWriter.close();
		}
	}

	public static void main(String args[]) throws Exception {
		if (args == null || args.length != 2) {
			System.err
					.println("args length must be 2: [nodeClusterMapfile] [outputDir]");
		}
		CommunityFinder finder = new CommunityFinder();
		finder.saveCommunity(finder.findCommunity(args[0]), args[1]);
	}
}
