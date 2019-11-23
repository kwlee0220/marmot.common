package marmot;

import marmot.exec.MarmotExecutionException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotSparkSession {
	public void createOrReplaceView(String viewName, String dsId);
	
	public void runSql(String sqlStmt, String outDsId, StoreDataSetOptions opts)
		throws MarmotExecutionException;
}
