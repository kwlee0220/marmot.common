package marmot.geo.catalog;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CatalogException extends RuntimeException {
	private static final long serialVersionUID = 3367187736008595374L;

	public CatalogException(String details) {
		super(details);
	}
	
	public CatalogException(Throwable cause) {
		super(cause);
	}
	
	public CatalogException(String details, Throwable cause) {
		super(details, cause);
	}
}
