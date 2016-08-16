package fastppv.core.hubselection;


public class OutDegHub extends UtilityHub {

	public OutDegHub(String nodeFile, String edgeFile) throws Exception {
		super(nodeFile, edgeFile, 0.0);
		this.pagerankPow = 0.0;
		this.outDegPow = 1.0;
	}
	

}
