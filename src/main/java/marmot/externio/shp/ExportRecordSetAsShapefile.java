package marmot.externio.shp;

import java.io.IOException;

import marmot.RecordSet;
import utils.Utilities;
import utils.react.ProgressiveExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportRecordSetAsShapefile extends ExportAsShapefile {
	private final RecordSet m_source;
	private final String m_srid;
	
	public ExportRecordSetAsShapefile(RecordSet source, String srid,
										String outputDir, ExportShapefileParameters params) {
		super(outputDir, params);
		
		Utilities.checkNotNullArgument(source, "source RecordSet is null");
		Utilities.checkNotNullArgument(srid, "SRID is null");
		
		m_source = source;
		m_srid = srid;
	}
	
	public ProgressiveExecution<Long,Long> start() throws IOException {
		return start(m_source, m_srid);
	}
}
