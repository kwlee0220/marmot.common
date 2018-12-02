package marmot;

import utils.async.ExecutableExecution;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class PlanExecution extends ExecutableExecution<Void> {
	public abstract Plan getPlan();
	public abstract RecordSchema getRecordSchema();
	
	public abstract void setDisableLocalExecution(boolean flag);
	public abstract void setMapOutputCompressCodec(String codec);
}
