package marmot.plan;

import marmot.Plan;
import marmot.exec.PlanExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PlanExecutionFactory {
	public PlanExecution create(Plan plan);
}
