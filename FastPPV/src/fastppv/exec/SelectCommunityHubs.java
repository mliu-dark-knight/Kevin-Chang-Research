package fastppv.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import fastppv.data.Node;
import fastppv.util.Config;
import fastppv.util.io.TextReader;
/**
 * input1: a clustering file with each line in the form nodeId clusterID
 * input2: all hubs needed
 * additional input: the number of hubs in each community
 * output: the hubs in each community
 * @author zhufanwei
 *
 */

public class SelectCommunityHubs {

	 
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
	 public List<String> parseRankedNodes(String nodesFile) throws Exception {
		// TODO Auto-generated method stub
		 List<String> rankedNodes = new ArrayList<String>();
		 TextReader in = new TextReader(nodesFile);
	        String line;
	        while ( (line = in.readln()) != null) {
	        	
	        	rankedNodes.add(line.trim());
	        }
	        in.close();
	        return rankedNodes;
	}
	 public Map<String, Integer> parseHubsNoComm(String hubsNoFile) throws Exception{
		 Map <String, Integer>hubsNoEachCom = new HashMap<String, Integer>(); 
		 TextReader in = new TextReader(hubsNoFile);
	        String line;
	        int lineNo = 0;
	        while ( (line = in.readln()) != null) {
	        	double hubs = Double.valueOf(line.trim());
	        	int roundedNo = (int) Math.round(hubs);
	        	hubsNoEachCom.put(String.valueOf(lineNo), roundedNo);
	        	lineNo ++;
	        }
	        in.close();
		 int totalHubs=0;
		 for(int hubsNo: hubsNoEachCom.values())
			 totalHubs+=hubsNo;
		 System.out.println("Total No of hubs:"+ totalHubs);
		 return hubsNoEachCom;
	 }
	 
		public void distributeHubs(String clusterMappingFile, String nodesFile, String hubsNoFile ) throws Exception {
			// TODO Auto-generated method stub
			
			Map<String, String> communityMapping=	parseNodeClusterMap(clusterMappingFile);
			List<String> nodes = parseRankedNodes(nodesFile);
			Map<String, List<String>> results = new HashMap<String, List<String>>();
			Map<String,Integer> commuHubs = parseHubsNoComm(hubsNoFile);
			for(String n : nodes){
				String communityId = communityMapping.get(n);
				if(results.get(communityId) == null){
					List<String> hubs = new ArrayList<String>();
					hubs.add(n);
					results.put(communityId, hubs);
				}
				else{
					results.get(communityId).add(n);
				}
			}
			
// print hubs in a community
			for(Map.Entry<String, Integer> e: commuHubs.entrySet()){
				String comID = e.getKey();
				List<String> rankedNodesInCommu= results.get(comID);
		    //    System.out.println(size +" hubs in Community "+ communityID);
		        for(int i = 0; i < e.getValue(); i++){
					System.out.println(rankedNodesInCommu.get(i));
				}
			}
			 
			
//			return results;
			
		}

	/**
	 * 	
	 * @param args
	 * @throws Exception
	 */
	 public static void main(String[] args) throws Exception {
		
	        String clusterMappingFile = args[0];
	        String rankedNodes = args[1];
	        String hubsNoinComm = args[2];
	        SelectCommunityHubs commHubs = new SelectCommunityHubs();
	        commHubs.distributeHubs(clusterMappingFile,rankedNodes,hubsNoinComm);
	        
	       
	        
	       
	    }


}
