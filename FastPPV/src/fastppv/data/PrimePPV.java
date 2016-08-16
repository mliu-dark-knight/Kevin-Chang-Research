package fastppv.data;

import java.util.*;

import fastppv.util.Config;
import fastppv.util.IndexManager;
import fastppv.util.MapCapacity;
import fastppv.util.io.*;

public class PrimePPV extends PPV {

	private List<Integer> hubs;

	public PrimePPV(int capacity) {
		super(capacity);
		hubs = new ArrayList<Integer>();
	}

	public PrimePPV() {
		super();
		hubs = new ArrayList<Integer>();
	}

	public int numHubs() {
		return hubs.size();
	}

	public int getHubId(int index) {
		return hubs.get(index);
	}
// when this method would be invoked????
	public void set(Node n, double value) {
		if (n.isHub)
			hubs.add(n.id);

		super.set(n.id, value);
	}

	public void trim(double clip) {
		int l = 0;
		for (double v : map.values()) {
			if (v > clip)
				l++;
		}

		Map<Integer, Double> newMap = new HashMap<Integer, Double>(
				MapCapacity.compute(l));
		for (int i : map.keySet()) {
			double v = map.get(i);
			if (v > clip)
				newMap.put(i, v);
		}

		this.map = newMap;

		List<Integer> list = new ArrayList<Integer>();
		for (int id : hubs) {
			if (this.map.containsKey(id)) {
				list.add(id);
			}
		}
		this.hubs = list;

	}

	public long computeStorageInBytes() {
		long nodeIdSize = (1 + hubs.size()) * 4;
		long mapSize = (1 + map.size()) * 4 + map.size() * 8;
		return nodeIdSize + mapSize;
	}
	
	
	

	public PPV computeCorrectPPV() {
		PPV correctPPVMap = new PPV();
		correctPPVMap.map.putAll(this.map);
		for (int i = 1; i < hubs.size(); i++) {
			correctPPVMap
					.set(hubs.get(i), this.get(hubs.get(i)) * Config.alpha);
		}
		return correctPPVMap;
	}

	public void saveToDisk(boolean doTrim) throws Exception {
		DataWriter out = new DataWriter(IndexManager.getPrimePPVFilename(hubs
				.get(0)));

		if (doTrim)
			trim(Config.clip);

		out.writeInteger(hubs.size());
		for (int i : hubs) {
			out.writeInteger(i);
		}

		out.writeInteger(map.size());

		for (int i : map.keySet()) {
			out.writeInteger(i);
			out.writeDouble(map.get(i));
		}
		out.close();
	}

	public void loadFromDisk(int nodeId) throws Exception {
		DataReader in = new DataReader(IndexManager.getPrimePPVFilename(nodeId));
		int n = in.readInteger();
		hubs = new ArrayList<Integer>(n);
		for (int i = 0; i < n; i++)
			hubs.add(in.readInteger());
		
		/*
		 * if nodeId.cid == in.readInteger().cid  then sameComuHubs.add(in.readInteger); ?????
		 * 
		 * parsing community file, construct <nodeId, communityId> map here?
		 */

		n = in.readInteger();
		this.map = new HashMap<Integer, Double>(MapCapacity.compute(n)); //also need to change this map, only contain the value of sameC hubs
		for (int i = 0; i < n; i++) {
			int id = in.readInteger();
			set(id, in.readDouble());
		}
		in.close();
	}

	public String getCountInfo() {
		int graphSize = map.size();
		int hubSize = hubs.size();

		return graphSize + "  " + hubSize ;
	}

	public int getHubSize(){
		return hubs.size();
	}
}
