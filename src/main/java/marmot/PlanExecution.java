package marmot;

import utils.async.Execution;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PlanExecution extends Execution<Void>, Runnable {
	public Plan getPlan();
	public RecordSchema getRecordSchema();
	
	public void setDisableLocalExecution(boolean flag);
}
