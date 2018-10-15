package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetNotExistsException extends RecordSetException {
	private static final long serialVersionUID = 5177902212736177606L;

	public RecordSetNotExistsException(String details) {
		super(details);
	}
	
	public RecordSetNotExistsException(Throwable cause) {
		super(cause);
	}
	
	public RecordSetNotExistsException(String details, Throwable cause) {
		super(details, cause);
	}
}
