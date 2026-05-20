package marmot.externio.shp;

import java.io.IOException;

import utils.Preconditions;
import utils.rx.ProgressiveExecution;

import marmot.MarmotRuntime;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;


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
		Preconditions.checkNotNullArgument(dsId, "dataset id");
		
		m_dsId = dsId;
	}
	
	public ProgressiveExecution<Long,Long> start(MarmotRuntime marmot) throws IOException {
		DataSet ds = marmot.getDataSet(m_dsId);
		GeometryColumnInfo info = ds.getGeometryColumnInfo();
		
		return start(ds.read(), info.srid());
	}
}
