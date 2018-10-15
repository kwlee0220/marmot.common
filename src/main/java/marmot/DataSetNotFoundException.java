package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetNotFoundException extends DataSetException {
	private static final long serialVersionUID = 6191966431080864900L;
	
	public DataSetNotFoundException(String name) {
		super(name);
	}
	
	public DataSetNotFoundException(String name, Throwable cause) {
		super(name, cause);
	}
}
