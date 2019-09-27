package marmot.exec;

import marmot.MarmotRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExecutionNotFoundException extends MarmotRuntimeException {
	private static final long serialVersionUID = -4103450046969415267L;

	public ExecutionNotFoundException(String details) {
		super(details);
	}
}
