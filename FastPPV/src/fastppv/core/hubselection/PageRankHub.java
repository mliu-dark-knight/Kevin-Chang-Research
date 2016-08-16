package fastppv.core.hubselection;


public class PageRankHub extends UtilityHub {

	public PageRankHub(String nodeFile, String edgeFile) throws Exception {
		super(nodeFile, edgeFile, 0.0);
		this.pagerankPow = 1.0;
		this.outDegPow = 0.0;
	}


}
