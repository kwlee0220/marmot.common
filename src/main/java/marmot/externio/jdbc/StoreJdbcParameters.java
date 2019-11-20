package marmot.externio.jdbc;

import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreJdbcParameters extends JdbcParameters {
	private GeometryFormat m_geomFormat = GeometryFormat.WKB;
	
	public static enum GeometryFormat {
		WKB, WKT
	}
	
	public GeometryFormat geometryFormat() {
		return m_geomFormat;
	}

	@Option(names={"-geom_format"}, paramLabel="geometry_format",
			description={"data format for geometry column"})
	public StoreJdbcParameters geometryFormat(String format) {
		m_geomFormat = GeometryFormat.valueOf(format.toUpperCase());
		return this;
	}
}
