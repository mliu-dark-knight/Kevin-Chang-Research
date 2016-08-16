package fastppv.exec;

import java.util.ArrayList;
import java.util.List;




import fastppv.data.Graph;
import fastppv.util.Config;
import fastppv.util.KeyValuePair;
import fastppv.util.io.TextReader;
import fastppv.util.io.TextWriter;


public class OnlineExact {

    public static void main(String[] args) throws Exception {

        Graph graph = new Graph();
        graph.loadFromFile(Config.nodeFile, Config.edgeFile, false);
        
        System.out.println("Loading queries...");
        List<Integer> qids = new ArrayList<Integer>();
        TextReader in = new TextReader(Config.queryFile);
        String line;
        while ( (line = in.readln()) != null) {
        	int qid = Integer.parseInt(line);
        	qids.add(qid);
        }
        in.close();
        
        System.out.println("Starting query processing...");
        
        TextWriter out = new TextWriter(Config.outputDir + "/exact");       
        for (int qid : qids) {
           	System.out.print("+");
            
            long start = System.currentTimeMillis();
            graph.computePageRank(qid);
            List<KeyValuePair> rankedResult = graph.toPPV().getTopResult(Config.resultTop);
            long elapsed = System.currentTimeMillis() - start;
            
            out.write(elapsed + "ms ");
            for (KeyValuePair e : rankedResult)
                out.write(e.key + "_" + e.value + " ");
            out.writeln();
        }
        out.close();
        
        System.out.println();
    }

}
