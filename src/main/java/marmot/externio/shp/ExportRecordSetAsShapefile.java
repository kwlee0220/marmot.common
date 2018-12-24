package marmot.externio.shp;

import java.io.IOException;
import java.util.Objects;

import marmot.RecordSet;
import utils.async.ProgressiveExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportRecordSetAsShapefile extends ExportAsShapefile {
	private final RecordSet m_source;
	private final String m_srid;
	
	public ExportRecordSetAsShapefile(RecordSet source, String srid,
										String outputDir, ShapefileParameters params) {
		super(outputDir, params);
		
		Objects.requireNonNull(source, "source RecordSet is null");
		Objects.requireNonNull(srid, "SRID is null");
		
		m_source = source;
		m_srid = srid;
	}
	
	public ProgressiveExecution<Long,Long> start() throws IOException {
		return start(m_source, m_srid);
	}
}
