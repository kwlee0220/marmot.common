package marmot.support;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBException extends RuntimeException {
	private static final long serialVersionUID = 6917539455484693259L;

	public PBException(Throwable cause) {
		super(cause);
	}

	public PBException(String msg) {
		super(msg);
	}

	public PBException(String msg, Throwable cause) {
		super(msg, cause);
	}
}
