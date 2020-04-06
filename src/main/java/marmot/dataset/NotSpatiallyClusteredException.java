package marmot.dataset;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NotSpatiallyClusteredException extends DataSetException {
	private static final long serialVersionUID = 1L;

	public NotSpatiallyClusteredException(String name) {
		super(name);
	}

	public NotSpatiallyClusteredException(String details, Throwable cause) {
		super(details, cause);
	}
}
