package fastppv.exec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import fastppv.data.Graph;
import fastppv.data.PPV;
import fastppv.util.Config;
import fastppv.util.IndexManager;
import fastppv.util.KeyValuePair;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;

public class SelectHubsComIntegrate {

	/**
	 * input1: a clustering file with each line in the form nodeId clusterID ==>nodeClusterMap 
	 * input2: number of total hubs ==> hubSize 
	 * input3: number of clusters ==> numOfClusters
	 * 
	 * @author zhufanwei
	 * 
	 */
	public Map<Integer, Integer> nodeClusterMap = new HashMap<Integer, Integer>();
	public Map<Integer, Integer> clusterNodesNumMap = new HashMap<Integer, Integer>();

	public void parseNodeClusterMap(String clusterMapFile) throws Exception {

		TextReader inN = new TextReader(clusterMapFile);
		String line;

		while ((line = inN.readln()) != null) {
			String s[] = line.split("\t");
			if (s == null || s.length != 2) {
				System.err.println("error node cluster map line:" + line);
				continue;
			}
			nodeClusterMap.put(Integer.valueOf(s[0]), Integer.valueOf(s[1]));
			System.out.println();
		}
		for (Map.Entry<Integer, Integer> e : nodeClusterMap.entrySet()) {
			if (clusterNodesNumMap.get(e.getValue()) == null) {
				clusterNodesNumMap.put(e.getValue(), 1);

			} else {
				clusterNodesNumMap.put(e.getValue(),
						clusterNodesNumMap.get(e.getValue()) + 1);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		List<String> hubs = new ArrayList<String>(); // set of hubs

		String clusterMappingFile = args[0];
		int hubSize = Integer.valueOf(args[1]);
		int numOfClusters = Integer.valueOf(args[2]);
		SelectHubsComIntegrate s = new SelectHubsComIntegrate();
		s.parseNodeClusterMap(clusterMappingFile);

		int[] clusterSelectedHubs = new int[numOfClusters];// index is cluster,
															// value is hubs.

		Graph graph = new Graph();
		graph.loadFromFile(Config.nodeFile, Config.edgeFile, false);
		ArrayList<KeyValuePair> nodesUtilityValues = new ArrayList<KeyValuePair>();

		graph.computePageRank(-1);
		PPV ppv = graph.toPPV();
		for (Entry<Integer, Double> e : ppv.getEntrySet()) {
			int id = e.getKey();
			double utility = Math.pow(e.getValue(), 1.0)
					* Math.pow(graph.getNode(id).out.size(), 1.0);
			int numOfnodes = s.clusterNodesNumMap.get(s.nodeClusterMap.get(id)); // num of nodes in its cluster
			nodesUtilityValues.add(new KeyValuePair(id, utility * numOfnodes));
		}

		// sort nodesKeyValuePair in desc order
		Collections.sort(nodesUtilityValues, new Comparator<KeyValuePair>() {
			@Override
			public int compare(KeyValuePair arg0, KeyValuePair arg1) {
				return -Double.compare(arg0.value, arg1.value);
			}
		});

		for (int i = 0; i < numOfClusters; i++) {
			clusterSelectedHubs[i] = 1;
		}
		/**
		 *  key->hub num  been selected,
		 *  value-> cluster nums with the ¡®key¡¯ value selected hubs
		 */
		Map<Integer, Integer> hubNumsClusterNumsMap = new HashMap<Integer, Integer>();
		hubNumsClusterNumsMap.put(1, numOfClusters);
		int minSelectHubs = 1;// store the min selected hubs.
		int nodeChoosedFlag[]=new int[nodesUtilityValues.size()];
		for(int i=0;i<nodeChoosedFlag.length;i++){
			nodeChoosedFlag[i]=0;
		}

		for (int i = 0; i < hubSize; i++) {
			double maxScore = 0;
			int hubId = 0;
			int selectIndex = 0;
			for (int j = 0; j < nodesUtilityValues.size(); j++) {
				if(nodeChoosedFlag[j]==1){//if node has already been selected as hub,ignore it
					continue;
				}
				KeyValuePair pair = nodesUtilityValues.get(j);
				int nodeId = pair.key;
				double score = pair.value;
				int selectHubs=clusterSelectedHubs[s.nodeClusterMap.get(nodeId)];
				double realScore = score
						/ selectHubs;
				if(selectHubs==minSelectHubs){
					if(maxScore>realScore){//no need to continue.
						break;
					}
				}
				if (realScore > maxScore) {
					maxScore = realScore;
					hubId = nodeId;
					selectIndex = j;
				}

			} // integratedScore = utility * numOfNodes in its community /num of
				// selected hub in its community

			hubs.add(String.valueOf(hubId));
			nodeChoosedFlag[selectIndex]=1;

			int selectedClusterId = s.nodeClusterMap.get(hubId);
			int selectedHubsOfSelectedCluster=clusterSelectedHubs[selectedClusterId] ;
			clusterSelectedHubs[selectedClusterId] =selectedHubsOfSelectedCluster + 1;
			updateHubNumsClusterNumsMap(hubNumsClusterNumsMap,selectedHubsOfSelectedCluster);
			minSelectHubs=getMinSelectedHubInCluster(minSelectHubs,hubNumsClusterNumsMap);//update minSelectHubs.
		}
		// saveToFile(hubs);

		for (int i = 0; i < hubs.size(); i++) {
			System.out.println(hubs.get(i));
		}

	}

	private static void updateHubNumsClusterNumsMap(
			Map<Integer, Integer> hubNumsClusterNumsMap, int i) {
		if (hubNumsClusterNumsMap.get(i) - 1 == 0) {
			hubNumsClusterNumsMap.remove(i);
		} else {
			hubNumsClusterNumsMap.put(i, hubNumsClusterNumsMap.get(i) - 1);
		}
		Integer newHubNums = i + 1;
		Integer clusterNums = hubNumsClusterNumsMap.get(newHubNums);
		if (clusterNums == null) {
			clusterNums = 1;
		}else{
			clusterNums=clusterNums+1;
		}
		hubNumsClusterNumsMap.put(newHubNums, clusterNums);

	}
	public static int getMinSelectedHubInCluster(int currentMinSelectHubs,Map<Integer,Integer> hubNumsClusterNumsMap){
		if(hubNumsClusterNumsMap.get(currentMinSelectHubs)!=null){
			return currentMinSelectHubs;
		}
		int newMinSelectHubs=currentMinSelectHubs+1;
		while(true){
			if(hubNumsClusterNumsMap.get(newMinSelectHubs)!=null){
				return newMinSelectHubs;
			}
			newMinSelectHubs++;
		}
	}

	private static void saveToFile(List<String> hubs) throws Exception {
		// TODO Auto-generated method stub
		IndexManager.mkSubDirShallow();

		TextWriter out = new TextWriter(IndexManager.getHubNodeFile());
		for (String s : hubs)
			out.writeln(s);
		out.close();
	}

}
