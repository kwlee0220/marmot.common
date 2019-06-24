package marmot;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetClosedException extends RecordSetException {
	private static final long serialVersionUID = 394448614552752730L;

	public RecordSetClosedException(String details) {
		super(details);
	}
}
