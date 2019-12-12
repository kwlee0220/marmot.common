package marmot.dataset;

import marmot.MarmotRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DataSetException extends MarmotRuntimeException {
	private static final long serialVersionUID = -3847213062132907407L;

	public DataSetException() {
		super();
	}

	public DataSetException(String details) {
		super(details);
	}

	public DataSetException(String details, Throwable cause) {
		super(details, cause);
	}
}
