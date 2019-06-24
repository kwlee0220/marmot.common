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
}
