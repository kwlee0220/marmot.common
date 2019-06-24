package marmot.support;

import marmot.MarmotRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBException extends MarmotRuntimeException {
	private static final long serialVersionUID = 6917539455484693259L;

	public PBException(String msg) {
		super(msg);
	}

	public PBException(Throwable cause) {
		super(cause);
	}

	public PBException(String details, Throwable cause) {
		super(details, cause);
	}
}
