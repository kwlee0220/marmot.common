package marmot.externio.shp;

import java.io.IOException;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import utils.Utilities;
import utils.async.ProgressiveExecution;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportDataSetAsShapefile extends ExportAsShapefile {
	private final String m_dsId;
	
	public static ExportDataSetAsShapefile create(String dsId, String output,
												ExportShapefileParameters params) {
		return new ExportDataSetAsShapefile(dsId, output, params);
	}
	
	public ExportDataSetAsShapefile(String dsId, String outputDir,
									ExportShapefileParameters params) {
		super(outputDir, params);
		Utilities.checkNotNullArgument(dsId, "dataset id");
		
		m_dsId = dsId;
	}
	
	public ProgressiveExecution<Long,Long> start(MarmotRuntime marmot) throws IOException {
		DataSet ds = marmot.getDataSet(m_dsId);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		
		return start(ds.read(), info.srid());
	}
}
