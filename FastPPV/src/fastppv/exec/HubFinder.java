package fastppv.exec;

import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.util.Config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author fw
 * @since 13-12-30
 */
public class HubFinder {
    public static int hubsNums = 0;
    public static Set<Integer> nodeAccessed = new HashSet<Integer>();


    public static void findNumOfHubsInGraph(Graph graph, int graphSize, int startNode) {
        Node n = graph.getNode(startNode);

        if (n == null) {
            System.err.println("can't found node :" + startNode);
        }

        List<Node> nodes = new ArrayList<Node>();
        nodes.add(n);
        nodeAccessed.add(n.id);
        if (n.isHub) {
            hubsNums++;
        }
        recursiveFindHubs(nodes, graphSize);
    }

    //
    public static void recursiveFindHubs(List<Node> startNodes, int graphSize) {
        List<Node> nextRoundStartNodes = new ArrayList<Node>();
        if (startNodes == null || startNodes.isEmpty()) {
            System.out.println("found end. no more neighbors. ");
        }
        for (Node n : startNodes) {
            if (n.out != null) {
                for (Node neighbor : n.out) {
                    if (nodeAccessed.contains(neighbor.id)) {    //
                        continue;
                    }

                    if (neighbor.isHub) {
                        hubsNums++;
                    }
                    nodeAccessed.add(neighbor.id);
                    if (nodeAccessed.size() >= graphSize) {
                        return;
                    }
                    nextRoundStartNodes.addAll(neighbor.out);

                }

            }
        }
        recursiveFindHubs(nextRoundStartNodes, graphSize);

    }


    public static void main(String args[]) throws Exception {
        int startNode = Integer.parseInt(args[0]);
        int graphSize = Integer.parseInt(args[1]);
        String hubNodeFile = args[2];

        Graph graph = new Graph();
        graph.loadFromFile(Config.nodeFile, Config.edgeFile, hubNodeFile);

        findNumOfHubsInGraph(graph, graphSize, startNode);

        System.out.println("hubs found:" + hubsNums +
                           "  startNode:" + startNode +
                           "  expect graph size:" + graphSize +
                           " real graph size:" + nodeAccessed.size());

    }

}
