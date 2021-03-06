package marmot;

import java.io.IOException;

import marmot.exec.MarmotExecutionException;
import marmot.optor.StoreDataSetOptions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotSparkSession extends MarmotRuntime {
	public void createOrReplaceView(String viewName, String dsId) throws IOException;
	public void runSql(String sqlStmt, String outDsId, StoreDataSetOptions opts)
		throws MarmotExecutionException;
}
