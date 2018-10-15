package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetTimeoutException extends RecordSetException {
	private static final long serialVersionUID = 1285682520952233025L;

	public RecordSetTimeoutException(String details) {
		super(details);
	}
}
