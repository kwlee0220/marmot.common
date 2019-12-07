package marmot.optor;

import marmot.exec.MarmotExecutionException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordSetOperatorException extends MarmotExecutionException {
	private static final long serialVersionUID = -6068486631747556824L;

	public RecordSetOperatorException(String details) {
		super(details);
	}

	public RecordSetOperatorException(String details, Throwable cause) {
		super(details, cause);
	}
	
//	public RecordSetOperatorException(RecordSetOperator op, Throwable cause) {
//		super(String.format("op=%s, cause=%s", op, cause));
//		
//	}
//
//	public RecordSetOperatorException(RecordSetOperator op, String details) {
//		super(String.format("op=%s, details=%s", op, details));
//	}
//
//	public RecordSetOperatorException(Class<? extends RecordSetOperator> opCls, String details) {
//		super(String.format("op=%s, details=%s", opCls.getSimpleName(), details));
//	}
}
