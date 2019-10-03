package marmot.exec;

import marmot.MarmotRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotExecutionException extends MarmotRuntimeException {
	private static final long serialVersionUID = 7397876817164231591L;

	public MarmotExecutionException(String details) {
		super(details);
	}

	public MarmotExecutionException(Throwable cause) {
		super(cause);
	}

	public MarmotExecutionException(String details, Throwable cause) {
		super(details, cause);
	}
}
