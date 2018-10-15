package marmot;

import utils.async.Execution;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PlanExecution extends Execution<Void>, AutoCloseable {
	public Plan getPlan();
	public RecordSchema getRecordSchema();
}
