package fastppv.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import fastppv.data.Graph;
import fastppv.data.Node;
import fastppv.data.PPV;
import fastppv.data.PrimePPV;
import fastppv.util.Config;

public class QueryProcessor {

	private Graph graph;
	private Map<Integer, Integer> communityMap;

	public QueryProcessor(Graph graph, Map<Integer, Integer> map) {
		this.graph = graph;
		this.communityMap = map;
		/*
		 * for(Entry<Integer,Integer>e: communityMap.entrySet())
		 * 
		 * System.out.println(e);
		 */
	}

	public PPV query(Node q) throws Exception {
		PrimePPV ppv;
		if (q.isHub) {
			ppv = new PrimePPV();
			ppv.loadFromDisk(q.id);
		} else {
			// StringBuilder sb = new StringBuilder();
			ppv = graph.computePrimePPV(q);
			// sb.append("queryID: "+q.id+ppv.getCountInfo()+"\n");
			// System.out.println("queryID: " + q.id + " " +
			// ppv.getCountInfo());

		}
		return doExpansionNoCorrection(ppv, q, Config.eta);
	}

	private PPV doExpansion(PrimePPV ppv, Node queryNode, int expansion)
			throws Exception {

		boolean isHub = queryNode.isHub;
		if (expansion == 0 || ppv.numHubs() == 0)
			return ppv.computeCorrectPPV();// multiply \alpha

		PPV result = ppv.duplicate();

		int startIndex = isHub ? 1 : 0;
		// added
		int qId = queryNode.id;
		// int crossComHubs =0;
		PPV hubScores = new PPV(ppv.numHubs());
		for (int j = startIndex; j < ppv.numHubs(); j++) {
			int h = ppv.getHubId(j); // get the id of each hub in this PPV
			// added
			hubScores.set(h, ppv.get(h));// get the value/score of that hub

		}
		// System.out.println(" # of unexpanded hubs" + crossComHubs);
		while (expansion > 0) {
			expansion--;
			PPV hubScoresNew = null;
			if (expansion > 0)
				hubScoresNew = new PPV();

			for (Entry<Integer, Double> e : hubScores.getEntrySet()) {
				double hs = e.getValue();
				int id = e.getKey();
				int cid_h = communityMap.get(id);
				int cid_q = communityMap.get(qId);
				if (hs < Config.delta || cid_h != cid_q) {
					/*
					 * if (cid_h != cid_q)
					 * 
					 * { System.out.println(" hub: " + id + " community: " +
					 * cid_h + " query: " + qId + " community: " + cid_q);
					 * System.out.println("---ppv score: " + hs + " delta: " +
					 * Config.delta); }
					 */

					result.set(id, result.get(id) - hs * (1 - Config.alpha));
					continue;
				}

				PrimePPV nextGraph = new PrimePPV();
				nextGraph.loadFromDisk(id);

				if (expansion == 0)
					result.addFrom(nextGraph.computeCorrectPPV(), hs);
				else
					result.addFrom(nextGraph, hs);

				result.set(id, result.get(id) - hs);

				if (expansion > 0) {

					for (int k = 1; k < nextGraph.numHubs(); k++) {//

						int h = nextGraph.getHubId(k);

						hubScoresNew.set(h,
								hubScoresNew.get(h) + nextGraph.get(h) * hs);

					}

				}

			}

			hubScores = hubScoresNew;
		}

		return result;
	}

	private PPV doExpansionNoCorrection(PrimePPV ppv, Node queryNode, int expansion)
			throws Exception {

		boolean isHub = queryNode.isHub;
		if (expansion == 0 || ppv.numHubs() == 0)
			//return ppv.computeCorrectPPV();// multiply \alpha
			return ppv;

		PPV result = ppv.duplicate();

		int startIndex = isHub ? 1 : 0;
		// added
		int qId = queryNode.id;
		// int crossComHubs =0;
		PPV hubScores = new PPV(ppv.numHubs());
		for (int j = startIndex; j < ppv.numHubs(); j++) {
			int h = ppv.getHubId(j); // get the id of each hub in this PPV
			// added
			hubScores.set(h, ppv.get(h));// get the value/score of that hub

		}
		// System.out.println(" # of unexpanded hubs" + crossComHubs);
		while (expansion > 0) {
			expansion--;
			PPV hubScoresNew = null;
			if (expansion > 0)
				hubScoresNew = new PPV();

			for (Entry<Integer, Double> e : hubScores.getEntrySet()) {
				double hs = e.getValue();
				int id = e.getKey();
				int cid_h = communityMap.get(id);
				int cid_q = communityMap.get(qId);
				if (hs < Config.delta || cid_h != cid_q) {
					/*
					 * if (cid_h != cid_q)
					 * 
					 * { System.out.println(" hub: " + id + " community: " +
					 * cid_h + " query: " + qId + " community: " + cid_q);
					 * System.out.println("---ppv score: " + hs + " delta: " +
					 * Config.delta); }
					 */

					//result.set(id, result.get(id) - hs * (1 - Config.alpha));
					continue;
				}

				PrimePPV nextGraph = new PrimePPV();
				nextGraph.loadFromDisk(id);

				/*if (expansion == 0)
					result.addFrom(nextGraph.computeCorrectPPV(), hs);
				else*/
				result.addFrom(nextGraph, hs);

				result.set(id, result.get(id) - hs);

				if (expansion > 0) {

					for (int k = 1; k < nextGraph.numHubs(); k++) {//

						int h = nextGraph.getHubId(k);

						hubScoresNew.set(h,
								hubScoresNew.get(h) + nextGraph.get(h) * hs);

					}

				}

			}

			hubScores = hubScoresNew;
		}

		return result;
	}

	private PPV doExpansion2(PrimePPV ppv, Node rootNode, int expansion,
			PPV curResult) throws Exception {

		// boolean isHub = queryNode.isHub;
		PPV result = curResult.duplicate();
		if (expansion == 0 || ppv.numHubs() == 0)
			return result;
	
		int startIndex = rootNode.isHub ? 1 : 0;

		for (int j = startIndex; j < ppv.numHubs(); j++) {
			int hid = ppv.getHubId(j); // get each border hub id 
			double hscoreInSub = ppv.get(hid);
			Node hubNode = graph.getNode(hid);
			if (hscoreInSub >= Config.delta) {
				double hscore_new = hscoreInSub * 0.15; // reset the value of
														// border hubs (*0.15)
				result.set(hid, hscore_new);
				PrimePPV borderHubPPV = new PrimePPV();
				borderHubPPV.loadFromDisk(hid);
				result.addFrom(borderHubPPV, hscoreInSub * 0.85); // assemble
																	// new ppv
			  
				doExpansion2(borderHubPPV, hubNode, expansion--, result); //expand from the borderhubs
			

			}else{
				continue;
			}

		}
		return result;
	}
	
}
