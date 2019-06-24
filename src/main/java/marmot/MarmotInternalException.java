package marmot;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotInternalException extends MarmotRuntimeException {
	private static final long serialVersionUID = -4430259396027306918L;

	public MarmotInternalException(String details) {
		super(details);
	}

	public MarmotInternalException(Throwable cause) {
		super(cause);
	}

	public MarmotInternalException(String details, Throwable cause) {
		super(details, cause);
	}
}
