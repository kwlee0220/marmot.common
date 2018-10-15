package marmot.optor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetOperatorException extends RuntimeException {
	private static final long serialVersionUID = 2269462235945624823L;

	public RecordSetOperatorException(String details) {
		super(details);
	}
	
	public RecordSetOperatorException(Throwable cause) {
		super(cause);
	}
	
	public RecordSetOperatorException(String details, Throwable cause) {
		super(details, cause);
	}
}
