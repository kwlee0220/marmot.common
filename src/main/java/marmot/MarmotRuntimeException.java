package marmot;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotRuntimeException extends RuntimeException {
	private static final long serialVersionUID = 563342113237555345L;

	public MarmotRuntimeException() {
		super();
	}

	public MarmotRuntimeException(String details) {
		super(details);
	}

	public MarmotRuntimeException(Throwable cause) {
		super(cause);
	}

	public MarmotRuntimeException(String details, Throwable cause) {
		super(details, cause);
	}
	
	public String getFullMessage() {
		String detailsMsg = (getMessage() != null) ? String.format(": %s", getMessage()) : "";
		String causeMsg = (getCause() != null) 	? String.format(", cause=%s", getCause()) : "";
		
		return String.format("%s%s%s", getClass().getName(), detailsMsg, causeMsg);
	}
}
