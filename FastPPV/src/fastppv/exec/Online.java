package fastppv.exec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;





import fastppv.core.QueryProcessor;
import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.util.Config;
import fastppv.util.KeyValuePair;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;


public class Online {

    public static void main(String[] args) throws Exception {
    	Config.hubType = args[0];
    	Config.numHubs = Integer.parseInt(args[1]);
    	Config.eta = Integer.parseInt(args[2]);    	
    	
        Graph graph = new Graph();
        graph.loadFromFile(Config.nodeFile, Config.edgeFile, true);
      //added  begin
        System.out.println("Parsing community...");
        /*
         * parse community file-mcl
         * 
         */
        Map<Integer, Integer > nodeClusterMap = new HashMap<Integer, Integer>();
		TextReader inC = new TextReader(Config.communityFile);
		String lineC;
	/*	int comID = 0;
		while ((lineC = inC.readln()) != null) {
			
			String s[] = lineC.trim().split("\t");
			for(String nodeId: s)
				nodeClusterMap.put(Integer.valueOf(nodeId),comID);
			
			comID++;
		}*/

		
		// parsing another type of community file-cluster
		while ((lineC = inC.readln()) != null) {
			String s[] = lineC.split("\t");
			if (s == null || s.length != 2) {
				System.err.println("error node cluster map line:" + lineC);
				continue;
			}
			nodeClusterMap.put(Integer.valueOf(s[0]), Integer.valueOf(s[1]));
		}
	
        
        //baseline file processing-- only one community
       /* while ((lineC = inC.readln()) != null) {
			
			nodeClusterMap.put(Integer.valueOf(lineC.trim()), 0);
		}
*/        
        inC.close();
 //added end       
        System.out.println("Loading queries...");
        List<Node> qNodes = new ArrayList<Node>();
        TextReader in = new TextReader(Config.queryFile);
        String line;
        while ( (line = in.readln()) != null) {
        	int id = Integer.parseInt(line);
        	qNodes.add(graph.getNode(id));
        }
        in.close();
        
        System.out.println("Starting query processing...");
        
        QueryProcessor qp = new QueryProcessor(graph,nodeClusterMap); //for community-based implementation; for the basic method, use  QueryProcessor qp = new QueryProcessor(graph);
       
        TextWriter out = new TextWriter(Config.outputDir + "/" + 
        		"fastppv-" + Config.hubType + "_" + Config.numHubs + "_" + Config.eta);
       
        int count = 0;
        for (Node q : qNodes) {
            count++;
            if (count % 10 == 0)
            	System.out.print("+");
            
            List<KeyValuePair> rankedResult = null;
            long start = System.currentTimeMillis();
            for (int i = 0; i < Config.numRepetitions; i++) {
            	rankedResult = qp.query(q).getTopResult(Config.resultTop);
            }            
            long elapsed = (System.currentTimeMillis() - start) / Config.numRepetitions;
            
            out.write(elapsed + "ms ");
            for (KeyValuePair e : rankedResult)
                out.write(e.key + "_" + e.value + " ");
            out.writeln();
        }
        out.close();
        
        System.out.println();
    }

}
