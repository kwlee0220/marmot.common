package marmot.externio.geojson;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Objects;

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
	private boolean m_wgs84 = false;
	
	public ExportAsGeoJson(String dsId) {
		Objects.requireNonNull(dsId, "dataset id is null");
		
		m_dsId = dsId;
	}
	
	public ExportAsGeoJson printPrinter(boolean flags) {
		m_prettyPrinter = flags;
		return this;
	}
	
	public ExportAsGeoJson wgs84(boolean flags) {
		m_wgs84 = flags;
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
		if ( m_wgs84 && !"EPSG:4326".equals(info.srid()) ) {
			builder.transformCrs(info.name(), info.srid(),
								"EPSG:4326", info.name());
		}
		
		return marmot.executeLocally(builder.build());
	}
}
