package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetExistsException extends DataSetException {
	private static final long serialVersionUID = 3306190940167709602L;

	public DataSetExistsException(String name) {
		super(name);
	}
	
	public DataSetExistsException(String name, Throwable cause) {
		super(name, cause);
	}
}
