# FastPPV

Run it as follows:

1. Select Hub nodes: 
	1) basic methods as in VLDB2013 paper: SelectHubs.java
	2) community-based hub selection (extended VLDBJ paper): SelectCommunityHubs.java
	
2. Offline precomputation: Offline.java
	Parameters:
	1) type of hubs (e.g. random, utility-1)
	2) number of precomputed hubs
	3) forceUpdate? (1 for true; 0 for false)
	
3. Online processing: Online.java
	Parameters:
	1) type of hubs (e.g. random, utility-1)
	2) number of precomputed hubs
	3) number of expansions (\eta)
	
4. Exact PPV computation: OnlineExact.java
   //Using the iterative method to compute exact PPV scores.
   
Note that, all the other parameters are specified in a config file (see Config.java).
   
