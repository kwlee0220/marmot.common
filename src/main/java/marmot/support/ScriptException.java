package marmot.support;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ScriptException extends RuntimeException {
	private static final long serialVersionUID = 4527470622716497423L;

	public ScriptException(Throwable cause) {
		super(cause);
	}
	
	public ScriptException(String details) {
		super(details);
	}
}
