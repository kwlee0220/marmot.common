package marmot.rset;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.geo.GeoClientUtils;
import marmot.support.DateFunctions;
import marmot.support.DateTimeFunctions;
import marmot.support.DefaultRecord;
import marmot.support.TimeFunctions;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcRecordAdaptor {
	private final RecordSchema m_schema;
	
	public JdbcRecordAdaptor(RecordSchema schema) {
		m_schema = schema;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public Record toRecord(ResultSet rs) {
		Record record = DefaultRecord.of(m_schema);
		loadRecord(rs, record);
		
		return record;
	}
	
	public void loadRecord(ResultSet rs, Record record) throws RecordSetException {
		RecordSchema schema = record.getRecordSchema();
		
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			try {
				Column col = schema.getColumnAt(i);
				int colIdx = i+1;
				switch ( col.type().getTypeCode() ) {
					case STRING:
						record.set(i, rs.getString(colIdx));
						break;
					case INT:
						record.set(i, rs.getInt(colIdx));
						break;
					case LONG:
						record.set(i, rs.getLong(colIdx));
						break;
					case DOUBLE:
						record.set(i, rs.getDouble(colIdx));
						break;
					case BINARY:
						record.set(i, rs.getBytes(colIdx));
						break;
					case FLOAT:
						record.set(i, rs.getFloat(colIdx));
						break;
					case DATETIME:
						record.set(i, DateTimeFunctions.DateTimeFromMillis(rs.getLong(colIdx)));
						break;
					case DATE:
						record.set(i, DateFunctions.DateFromMillis(rs.getLong(colIdx)));
						break;
					case TIME:
						record.set(i, TimeFunctions.TimeFromString(rs.getString(colIdx)));
						break;
					case POLYGON:
					case MULTI_POLYGON:
					case POINT:
					case MULTI_POINT:
					case LINESTRING:
					case MULTI_LINESTRING:
					case GEOM_COLLECTION:
					case GEOMETRY:
						Geometry geom = readGeometry(schema, rs, colIdx);
						record.set(i, geom);
						break;
					case BOOLEAN:
						record.set(i, rs.getBoolean(colIdx));
						break;
					default:
						throw new RecordSetException("unexpected DataType: " + col);
				}
			}
			catch ( SQLException e ) {
				Column col = schema.getColumnAt(i);
				throw new RecordSetException("fails to load column: column=" + col, e);
			}
		}
	}

	public void storeRecord(Record record, PreparedStatement pstmt) throws RecordSetException {
		RecordSchema schema = record.getRecordSchema();
		Object[] values = record.getAll();

		String str;
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			try {
				Column col = schema.getColumnAt(i);
				switch ( col.type().getTypeCode() ) {
					case STRING:
						// PostgreSQL의 경우 문자열에 '0x00'가 포함되는 경우
						// 오류를 발생시키기 때문에, 삽입전에 제거시킨다.
						str = (String)values[i];
						if ( str != null ) {
							str = str.replaceAll("\\x00","");
						}
						pstmt.setString(i+1, str);
						break;
					case INT:
						pstmt.setInt(i+1, (Integer)values[i]);
						break;
					case LONG:
						pstmt.setLong(i+1, (Long)values[i]);
						break;
					case SHORT:
						pstmt.setShort(i+1, (Short)values[i]);
						break;
					case DOUBLE:
						pstmt.setDouble(i+1, (Double)values[i]);
						break;
					case BINARY:
						pstmt.setBytes(i+1, (byte[])values[i]);
						break;
					case FLOAT:
						pstmt.setFloat(i+1, (Float)values[i]);
						break;
					case DATETIME:
						pstmt.setLong(i+1, DateTimeFunctions.DateTimeToMillis(values[i]));
						break;
					case DATE:
						pstmt.setLong(i+1, DateFunctions.DateToMillis(values[i]));
						break;
					case TIME:
						pstmt.setString(i+1, ((LocalTime)values[i]).toString());
						break;
					case POLYGON:
					case MULTI_POLYGON:
					case POINT:
					case MULTI_POINT:
					case LINESTRING:
					case MULTI_LINESTRING:
					case GEOM_COLLECTION:
					case GEOMETRY:
						writeGeometry(schema, pstmt, i+1, (Geometry)values[i]);
						break;
					case BOOLEAN:
						pstmt.setBoolean(i+1, (Boolean)values[i]);
						break;
					default:
						throw new RecordSetException("unexpected DataType: " + col);
				}
			}
			catch ( SQLException e ) {
				Column col = schema.getColumnAt(i);
				throw new RecordSetException("fails to store column: " + col, e);
			}
		}
	}
	
	public static DataType fromSqlType(int type, String typeName) {
		switch ( type ) {
			case Types.VARCHAR:
				return DataType.STRING;
			case Types.INTEGER:
				return DataType.INT;
			case Types.DOUBLE:
			case Types.NUMERIC:
				return DataType.DOUBLE;
			case Types.FLOAT:
			case Types.REAL:
				return DataType.FLOAT;
			case Types.BINARY:
			case Types.VARBINARY:
				return DataType.BINARY;
			case Types.BIGINT:
				return DataType.LONG;
			case Types.BOOLEAN:
				return DataType.BOOLEAN;
			case Types.SMALLINT:
				return DataType.SHORT;
			case Types.TINYINT:
				return DataType.BYTE;
			default:
				throw new IllegalArgumentException("unsupported SqlTypes: type=" + typeName
													+ ", code=" + type);
		}
	}
	
	protected Geometry readGeometry(RecordSchema schema, ResultSet rs, int colIdx)
		throws SQLException {
		try {
			return GeoClientUtils.fromWKB(rs.getBytes(colIdx));
		}
		catch ( ParseException e ) {
			Column col = schema.getColumnAt(colIdx);
			throw new RecordSetException("fails to parse WKB: column=" + col, e);
		}
	}
		
	protected void writeGeometry(RecordSchema schema, PreparedStatement pstmt,
									int colIdx, Geometry geom) throws SQLException {
		pstmt.setBytes(colIdx, GeoClientUtils.toWKB(geom));
	}
}
