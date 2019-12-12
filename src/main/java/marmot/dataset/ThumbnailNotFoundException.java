package marmot.dataset;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ThumbnailNotFoundException extends DataSetException {
	private static final long serialVersionUID = -6394193171338027663L;

	public ThumbnailNotFoundException(String name) {
		super(name);
	}
}
