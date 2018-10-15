package marmot;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PlanExecutionException extends RuntimeException {
	private static final long serialVersionUID = 7397876817164231591L;

	public PlanExecutionException(String details) {
		super(details);
	}
	
	public PlanExecutionException(Throwable cause) {
		super("" + cause);
	}
	
	public PlanExecutionException(String details, Throwable cause) {
		super("details: " + cause);
	}
}
