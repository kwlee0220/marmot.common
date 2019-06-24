package marmot.geo.catalog;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CatalogException extends MarmotInternalException {
	private static final long serialVersionUID = 3367187736008595374L;

	public CatalogException(String details) {
		super(details);
	}

	public CatalogException(Throwable cause) {
		super(cause);
	}
}
