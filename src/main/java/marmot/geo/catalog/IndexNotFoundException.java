package marmot.geo.catalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IndexNotFoundException extends CatalogException {
	private static final long serialVersionUID = -7984551083913014415L;

	public static IndexNotFoundException fromDataSet(String dsId) {
		String msg = String.format("dataset:%s", dsId);
		return new IndexNotFoundException(msg);
	}

	public IndexNotFoundException(String name) {
		super("index name: " + name);
	}
}
