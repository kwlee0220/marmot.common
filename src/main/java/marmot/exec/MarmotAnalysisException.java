package marmot.exec;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotAnalysisException extends MarmotInternalException {
	private static final long serialVersionUID = -4674959737263867153L;

	public MarmotAnalysisException(String details) {
		super(details);
	}

	public MarmotAnalysisException(Throwable cause) {
		super(cause);
	}
}
