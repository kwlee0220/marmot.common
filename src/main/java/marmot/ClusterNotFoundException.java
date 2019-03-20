package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterNotFoundException extends DataSetException {
	private static final long serialVersionUID = 6078129569385050323L;

	public static ClusterNotFoundException fromDataSet(DataSet ds) {
		String msg = String.format("cluster not found: dataset=%s", ds.getId());
		return new ClusterNotFoundException(msg);
	}
	
	public ClusterNotFoundException(String msg) {
		super(msg);
	}
}
