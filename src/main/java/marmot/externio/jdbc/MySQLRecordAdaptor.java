package marmot.externio.jdbc;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import marmot.Column;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.type.DataType;
import marmot.type.TypeCode;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MySQLRecordAdaptor extends JdbcRecordAdaptor {
	public MySQLRecordAdaptor(RecordSchema schema, GeometryFormat format) {
		super(schema, format);
	}

	@Override
	protected String declareSQLColumn(Column col) {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			switch ( getGeometryFormat() ) {
				case NATIVE:
					return String.format("%s GEOMETRY", col.name());
				case WKB:
					return String.format("%s longblob", col.name());
				case WKT:
					return String.format("%s text", col.name());
				default:
					throw new IllegalArgumentException("unsupported GeometryFormat: " + getGeometryFormat());
			}
		}
		else if ( type.getTypeCode() == TypeCode.BINARY || type.getTypeCode() == TypeCode.TYPED ) {
			return String.format("%s varbinary", col.name());
		}
		else {
			return super.declareSQLColumn(col);
		}
	}

	@Override
	protected Geometry getGeometryColumn(Column col, ResultSet rs, int colIdx)
		throws SQLException, IOException {
		try {
			switch ( getGeometryFormat() ) {
				case WKB:
					return GeoClientUtils.fromWKB(rs.getBinaryStream(colIdx));
				case WKT:
					return GeoClientUtils.fromWKT(rs.getString(colIdx));
				default:
					throw new IllegalArgumentException("unsupported GeometryFormat: " + getGeometryFormat());
			}
		}
		catch ( ParseException e ) {
			throw new IOException("" + e);
		}
	}

	@Override
	protected void setGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		switch ( getGeometryFormat() ) {
			case WKB:
				pstmt.setBytes(idx, GeoClientUtils.toWKB(geom));
				break;
			case WKT:
			case NATIVE:
				pstmt.setString(idx, GeoClientUtils.toWKT(geom));
				break;
			default:
				throw new IllegalArgumentException("unsupported GeometryFormat: " + getGeometryFormat());
		}
	}

	@Override
	public String getInsertValueExpr(Column col) {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			switch ( getGeometryFormat() ) {
				case NATIVE:
					return "GeomFromText(?)";
				default:
					return super.getInsertValueExpr(col);
			}
		}
		else {
			return super.getInsertValueExpr(col);
		}
	}
}
