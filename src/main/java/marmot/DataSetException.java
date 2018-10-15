package marmot;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetException extends RuntimeException {
	private static final long serialVersionUID = -3847213062132907407L;

	public DataSetException() {
		super();
	}

	public DataSetException(String details) {
		super(details);
	}
	
	public DataSetException(Throwable cause) {
		super(cause);
	}
	
	public DataSetException(String details, Throwable cause) {
		super(details, cause);
	}
}
