package fastppv.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;
/*
 * given para1: a community file with each line representing the nodes in a community and para2: a nodes file with each line representing a node (could be all nodes on the graph, or just query nodes)
 * output the number of nodes in each community
 */
public class NodesDistribution {

    public Map<String, String> parseNodeClusterMap(String clusterMappingFile) {
        Map<String, String> nodeClusterMap = new HashMap<String, String>();
        try {
            TextReader txtReader = new TextReader(clusterMappingFile);
            String line;
            while ((line = txtReader.readln()) != null) {
            	if (line.startsWith("#"))
    				continue;
                String s[] = line.split("\t");
                nodeClusterMap.put(s[0].trim(), s[1].trim());
              //  line = txtReader.readln();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return nodeClusterMap;
    }

    public List<String> parsequeryNodes(String queryFIle) {
        List<String> queryNodes = new ArrayList<String>();
        try {
            TextReader txtReader = new TextReader(queryFIle);
            String line = txtReader.readln();
            while (line != null) {
                queryNodes.add(line.trim());
                line = txtReader.readln();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return queryNodes;
    }

    public Map<String, Integer> countQuery(Map<String, String> nodeClusterMap, List<String> queryNodes) {
        Map<String, Integer> clusterCount = new HashMap<String, Integer>();

        for (String query : queryNodes) {
            String cluster = nodeClusterMap.get(query);
            if (cluster != null) {
                int result = clusterCount.get(cluster) == null ? 1 : clusterCount.get(cluster) + 1;
                clusterCount.put(cluster, result);
            }
        }

        return clusterCount;
    }

    public void process(String clusterMappingFile, String queryFIle) {
        Map<String, String> nodeClusterMap = parseNodeClusterMap(clusterMappingFile);
        List<String> queryNodes = parsequeryNodes(queryFIle);
        Map<String, Integer> result = countQuery(nodeClusterMap, queryNodes);
        try {
        //    TextWriter writer = new TextWriter(outputFile);
        	System.out.println("communityID" + "# of nodes");
        	for (Entry<String, Integer> e : result.entrySet()) {
            	
               System.out.println(e.getKey() + " " + e.getValue() );
            }
        //    writer.close();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // !TODO save result
    }

    public static void main(String[] args) {
        String clusterMappingFile = args[0];
        String queryFile = args[1]; 
        NodesDistribution queryDis = new NodesDistribution();
        queryDis.process(clusterMappingFile, queryFile);
    }
}
