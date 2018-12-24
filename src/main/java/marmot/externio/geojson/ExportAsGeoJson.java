package marmot.externio.geojson;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nullable;

import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.PlanBuilder;
import marmot.RecordSet;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportAsGeoJson {
	private final String m_dsId;
	private boolean m_prettyPrinter = false;
	@Nullable private String m_gjsonSrid;
	
	public ExportAsGeoJson(String dsId) {
		Objects.requireNonNull(dsId, "dataset id is null");
		
		m_dsId = dsId;
	}
	
	public ExportAsGeoJson printPrinter(boolean flags) {
		m_prettyPrinter = flags;
		return this;
	}
	
	public ExportAsGeoJson setGeoJSONSrid(String srid) {
		m_gjsonSrid = srid;
		return this;
	}
	
	public long run(MarmotRuntime marmot, BufferedWriter writer) throws IOException {
		GeometryColumnInfo info = marmot.getDataSet(m_dsId).getGeometryColumnInfo();
		
		try ( RecordSet rset = locateRecordSet(marmot, info);
			GeoJsonRecordSetWriter gwriter = GeoJsonRecordSetWriter.get(writer) ) {
			return gwriter.geometryColumn(info.name())
							.prettyPrinter(m_prettyPrinter)
							.write(rset);
		}
	}
	
	private RecordSet locateRecordSet(MarmotRuntime marmot, GeometryColumnInfo info) {
		PlanBuilder builder = marmot.planBuilder("export_geojson")
										.load(m_dsId);
		if ( m_gjsonSrid != null && !m_gjsonSrid.equals(info.srid()) ) {
			builder.transformCrs(info.name(), info.srid(), m_gjsonSrid);
		}
		
		return marmot.executeLocally(builder.build());
	}
}
