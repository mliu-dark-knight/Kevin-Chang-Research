package fastppv.core.hubselection;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import fastppv.data.Graph;
import fastppv.util.Config;
import fastppv.util.IndexManager;
import fastppv.util.KeyValuePair;
import fastppv.util.io.TextWriter;

public abstract class HubSelection {

	protected List<KeyValuePair> nodes;
	protected Graph graph;

	protected abstract void fillNodes();

	public HubSelection(String nodeFile, String edgeFile) throws Exception {
		graph = new Graph();
		graph.loadFromFile(nodeFile, edgeFile, false);
	}

	public void sortNodes() {
		Collections.sort(nodes, new Comparator<KeyValuePair>() {
			@Override
			public int compare(KeyValuePair arg0, KeyValuePair arg1) {
				return -Double.compare(arg0.value, arg1.value);
			}
		});

	}

	public void save() throws Exception {
		fillNodes();

		IndexManager.mkSubDirShallow();

		sortNodes();
		
		if (nodes.size() > Config.hubTop)
			nodes = nodes.subList(0, Config.hubTop);

		TextWriter out = new TextWriter(IndexManager.getHubNodeFile());
		for (KeyValuePair n : nodes)
			out.writeln(n.key);
		out.close();
	}
}
