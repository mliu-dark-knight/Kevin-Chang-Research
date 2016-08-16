package fastppv.core.hubselection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import fastppv.data.PPV;
import fastppv.util.Config;
import fastppv.util.KeyValuePair;

public class ProgressiveHub extends UtilityHub {

	public ProgressiveHub(String nodeFile, String edgeFile, double outDegPow)
			throws Exception {
		super(nodeFile, edgeFile, outDegPow);
		// TODO Auto-generated constructor stub
	}

//    public int batchSize;
//
//    public ProgressiveHub(String nodeFile, String edgeFile, double outDegPow) throws Exception{
//        super(nodeFile, edgeFile, outDegPow);
//        batchSize = (int) Math.ceil(Config.hubTop / Config.iterations);
//    }
//
//    @Override
//    protected void fillNodes() {
//        long start = System.currentTimeMillis();
//        List<KeyValuePair> selectedNodes = new ArrayList<KeyValuePair>();
//        super.fillNodes();// invoke utility rank
//        sortNodes();
//        List<KeyValuePair> sortedUtilityNodes = copyNodes(nodes);
//
//
//        Set<Integer> selectedHubIds = new HashSet<Integer>();// store selected
//                                                             // hubs by
//                                                             // progressive selection
//
//        /*
//         * int index = 1; for (KeyValuePair pair : nodes) { utilityRank.put(pair.key, index); index++; }
//         */
//
//        // add the 1st batch of nodes to selected nodes.
//        if (nodes.size() > batchSize) {
//            for (int i = 0; i < batchSize; i++) {
//                KeyValuePair pair = new KeyValuePair(nodes.get(i).key, nodes.get(i).value);
//                selectedNodes.add(pair);
//                selectedHubIds.add(nodes.get(i).key);
//            }
//        }
//        // int k=Config.progressiveTopK;
//        // select the next batch
//        for (int i = 0; i < Config.iterations - 1; i++) {
//
//            List<KeyValuePair> candidatePPVrank = new ArrayList<KeyValuePair>();
//
//            graph.computeMultiPPV(selectedNodes);// compute ppv of current hubs
//
//            PPV ppvs = graph.toPPV();
//
//            List<KeyValuePair> tempPairs = ppvs.getTopResult(nodes.size());
//
//            Map<Integer, Integer> indexMap = new HashMap<Integer, Integer>();
//            int index = 1;
//            for (int j = 0; j < tempPairs.size(); j++) {
//                if (selectedHubIds.contains(tempPairs.get(j).key)) continue;
//                candidatePPVrank.add(tempPairs.get(j));
//                indexMap.put(tempPairs.get(j).key, index);
//                index++;
//            }
//
//            /*
//             * for (Map.Entry<Integer, Double> e:ppvs.getEntrySet()) { int nodeid = e.getKey(); double ppv =
//             * e.getValue(); if (selectedHubIds.contains(nodeid)) continue; candidatePPVrank.add(nodeid); } // sort
//             * Collections.sort(candidatePPVrank, new Comparator<Integer>() {
//             * @Override public int compare(Integer arg0, Integer arg1) { return -Double.compare(arg0, arg1); } }); //
//             * sortNodes();
//             */int selected = 0;
//
//            for (int j = (i + 1) * batchSize + 1; j < sortedUtilityNodes.size(); j++) {
//                if (selectedHubIds.contains(sortedUtilityNodes.get(j).key)) {
//                    continue;
//                }
//                if (indexMap.get(sortedUtilityNodes.get(j).key)==null||indexMap.get(sortedUtilityNodes.get(j).key) > Config.progressiveTopK) {
//                    KeyValuePair pair = new KeyValuePair(sortedUtilityNodes.get(j).key, sortedUtilityNodes.get(j).value);
//                    selectedNodes.add(pair);
//                    selectedHubIds.add(sortedUtilityNodes.get(j).key);
//                    selected++;
//                    if (selectedNodes.size() >= Config.hubTop || selected >= batchSize) {
//                        break;
//                    }
//
//                }
//
//            }
//            // .....
//        }
//
//        nodes = selectedNodes;
//        long end = System.currentTimeMillis();
//
//        System.out.println("progressive hub selection time used:" + (end - start));
//
//    }
//
//    private List<KeyValuePair> copyNodes(List<KeyValuePair> nodes) {
//        List<KeyValuePair> copy = new ArrayList<KeyValuePair>();
//        for (KeyValuePair pair : nodes) {
//            KeyValuePair newNode = new KeyValuePair(pair.key,pair.value);
//            copy.add(newNode);
//        }
//        return copy;
//    }

}
