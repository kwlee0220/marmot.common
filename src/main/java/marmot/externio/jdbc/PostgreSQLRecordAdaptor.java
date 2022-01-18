package marmot.externio.jdbc;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.postgis.PGgeometry;

import marmot.Column;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.type.DataType;
import marmot.type.TypeCode;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PostgreSQLRecordAdaptor extends JdbcRecordAdaptor {
	public static final String JDBC_DRIVER_CLASS = "org.postgresql.Driver";
	
	public PostgreSQLRecordAdaptor(RecordSchema schema, GeometryFormat format) {
		super(schema, format);
	}

	@Override
	protected String declareSQLColumn(Column col) {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			switch ( type.getTypeCode() ) {
				case POLYGON:
					return String.format("%s geometry(Polygon)", col.name());
				case MULTI_POLYGON:
					return String.format("%s geometry(MultiPolygon)", col.name());
				case POINT:
					return String.format("%s geometry(Point)", col.name());
				case MULTI_POINT:
					return String.format("%s geometry(MultiPoint)", col.name());
				case LINESTRING:
					return String.format("%s geometry(LineString)", col.name());
				case MULTI_LINESTRING:
					return String.format("%s geometry(MultiLineString)", col.name());
				case GEOM_COLLECTION:
					return String.format("%s geometry(GeometryCollection)", col.name());
				case GEOMETRY:
					return String.format("%s geometry", col.name());
				default:
					throw new AssertionError("invalid GeometryType: " + type);
			}
		}
		else if ( type.getTypeCode() == TypeCode.BINARY || type.getTypeCode() == TypeCode.TYPED ) {
			return String.format("%s bytea", col.name());
		}
		else {
			return super.declareSQLColumn(col);
		}
	}

	@Override
	protected Geometry getGeometryColumn(Column col, ResultSet rs, int colIdx)
		throws SQLException, IOException {
		if ( getGeometryFormat() != GeometryFormat.NATIVE ) {
			return super.getGeometryColumn(col, rs, colIdx);
		}
		
		try {
			PGgeometry geom = (PGgeometry)rs.getObject(colIdx);
			String wkt = geom.getValue();
			return GeoClientUtils.fromWKT(wkt);
		}
		catch ( ParseException e ) {
			throw new IOException("" + e);
		}
	}

	@Override
	protected void setGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		if ( getGeometryFormat() != GeometryFormat.NATIVE ) {
			super.setGeometryColumn(pstmt, idx, col, geom);
		}
		else {
			pstmt.setObject(idx, new PGgeometry(GeoClientUtils.toWKT(geom)));	
		}
	}
}
