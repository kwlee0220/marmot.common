package marmot.io;

import marmot.MarmotRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileException extends MarmotRuntimeException {
	private static final long serialVersionUID = -8064054274154316855L;

	public MarmotFileException(String details) {
		super(details);
	}

	public MarmotFileException(Throwable cause) {
		super(cause);
	}

	public MarmotFileException(String details, Throwable cause) {
		super(details, cause);
	}
}
