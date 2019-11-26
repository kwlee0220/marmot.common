package marmot;

import marmot.exec.MarmotExecutionException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotSparkSession {
	public void execute(Plan plan, ExecutePlanOptions opts)
		throws MarmotExecutionException;
	public default void execute(Plan plan) throws MarmotExecutionException {
		execute(plan, ExecutePlanOptions.DEFAULT);
	}
	
	public void createOrReplaceView(String viewName, String dsId);
	public void runSql(String sqlStmt, String outDsId, StoreDataSetOptions opts)
		throws MarmotExecutionException;
}
