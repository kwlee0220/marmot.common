package marmot.externio.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import com.google.common.collect.Maps;

import utils.LocalDateTimes;
import utils.LocalDates;
import utils.LocalTimes;
import utils.jdbc.JdbcProcessor;
import utils.stream.KVFStream;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.geo.GeoClientUtils;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class JdbcRecordAdaptor {
	private final RecordSchema m_schema;
	private final GeometryFormat m_geomFormat;
	
	public static JdbcRecordAdaptor createDefault(RecordSchema schema, GeometryFormat format) {
		return new DefaultRecordAdaptor(schema, format);
	}
	
	public static JdbcRecordAdaptor create(JdbcProcessor jdbc, RecordSchema schema, GeometryFormat format) {
		Class<? extends JdbcRecordAdaptor> procCls = getAdaptorClass(jdbc.getSystem());
		
		try {
			Constructor<? extends JdbcRecordAdaptor> ctor
								= procCls.getConstructor(RecordSchema.class, GeometryFormat.class);
			return ctor.newInstance(schema, format);
		}
		catch ( Exception e ) {
			throw new IllegalArgumentException("fails to load JdbcRecordAdaptor, system="
												+ jdbc.getSystem() + ", cause=" + e);
		}
	}
	
	protected JdbcRecordAdaptor(RecordSchema schema, GeometryFormat format) {
		m_schema = schema;
		m_geomFormat = format;
	}
	
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	public GeometryFormat getGeometryFormat() {
		return m_geomFormat;
	}
	
	public static RecordSchema buildRecordSchema(JdbcProcessor jdbc, String tblName) throws SQLException {
		return KVFStream.from(jdbc.getColumns(tblName))
						.mapValue((k,v) -> fromSqlType(v.type(), v.typeName()))
						.map((k,v) -> new Column(k,v))
						.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c))
						.build();
	}
	
	public void createTable(JdbcProcessor jdbc, String tblName, String... primaryKeys)
		throws SQLException {
		StringBuilder builder = new StringBuilder(String.format("create table %s (", tblName));
		String prmKeyStr = "";
		if ( primaryKeys.length > 0 ) {
			prmKeyStr = Arrays.stream(primaryKeys)
								.collect(Collectors.joining(",", ", primary key (", ")"));
		}
		String sqlStr = builder.append(m_schema.streamColumns()
												.map(this::declareSQLColumn)
												.join(","))
								.append(prmKeyStr)
								.append(")")
								.toString();
		jdbc.executeUpdate(sqlStr);
	}
	
	public void loadRecord(ResultSet rs, Record record) throws RecordSetException {
		for ( int i =0; i < m_schema.getColumnCount(); ++i ) {
			Column col = m_schema.getColumnAt(i);
			record.set(i, getColumn(col, rs, i+1));
		}
	}

	public void storeRecord(Record record, PreparedStatement pstmt) throws RecordSetException {
		RecordSchema schema = record.getRecordSchema();
		Object[] values = record.getAll();

		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			Column col = schema.getColumnAt(i);
			setColumn(pstmt, i+1, col, values[i]);
		}
	}
	
	public static DataType fromSqlType(int type, String typeName) {
		switch ( type ) {
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
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
			case Types.LONGVARBINARY:
				return DataType.BINARY;
			case Types.BIGINT:
				return DataType.LONG;
			case Types.BOOLEAN:
				return DataType.BOOLEAN;
			case Types.SMALLINT:
				return DataType.SHORT;
			case Types.TINYINT:
				return DataType.BYTE;
			case Types.TIMESTAMP:
				return DataType.DATETIME;
			case Types.OTHER:
				if ( typeName.equals("geometry") ) {
					return DataType.GEOMETRY;
				}
				else {
					return DataType.NULL;
				}
			default:
				throw new IllegalArgumentException("unsupported SqlTypes: type=" + typeName
													+ ", code=" + type);
		}
	}
	
	protected String declareSQLColumn(Column col) {
		switch ( col.type().getTypeCode() ) {
			case STRING:
			case ENVELOPE:
				return String.format("%s text", col.name());
			case INT:
				return String.format("%s int", col.name());
			case LONG:
				return String.format("%s bigint", col.name());
			case DOUBLE:
				return String.format("%s double precision", col.name());
			case DATETIME:
				return String.format("%s bigint", col.name());
			case DATE:
				return String.format("%s bigint", col.name());
			case TIME:
				return String.format("%s varchar", col.name());
			case BOOLEAN:
				return String.format("%s boolean", col.name());
			case BYTE:
				return String.format("%s tinyint", col.name());
			case FLOAT:
				return String.format("%s float", col.name());
			case SHORT:
				return String.format("%s smallint", col.name());
			case BINARY:
			case TYPED:
				return String.format("%s varbinary", col.name());
			default:
				throw new RecordSetException("unsupported DataType: " + col);
		}
	}
	
	protected Geometry getGeometryColumn(Column col, ResultSet rs, int colIdx)
		throws SQLException, IOException {
		try {
			switch ( m_geomFormat ) {
				case WKB:
					try ( InputStream is = rs.getBinaryStream(colIdx) ) {
						return GeoClientUtils.fromWKB(is);
					}
				case WKT:
					return GeoClientUtils.fromWKT(rs.getString(colIdx));
				default:
					throw new IllegalStateException("unsupported GeometryFormat: " + m_geomFormat);
			}
		}
		catch ( ParseException e ) {
			throw new IOException("" + e);
		}
	}
	
	protected void setGeometryColumn(PreparedStatement pstmt, int idx, Column col, Geometry geom)
		throws SQLException {
		switch ( m_geomFormat ) {
			case WKB:
				byte[] wkb = GeoClientUtils.toWKB(geom);
				pstmt.setBytes(idx, wkb);
				break;
			case WKT:
				String wkt = GeoClientUtils.toWKT(geom);
				pstmt.setString(idx, wkt);
				break;
			default:
				throw new IllegalStateException("unsupported GeometryFormat: " + m_geomFormat);
		}
	}
	
	public String getInsertValueExpr(Column col) {
		return "?";
	}
	
	private Object getColumn(Column col, ResultSet rs, int colIdx) throws RecordSetException {
		try {
			if ( col.type().isGeometryType() ) {
				return getGeometryColumn(col, rs, colIdx);
			}
			else {
				switch ( col.type().getTypeCode() ) {
					case STRING:
						return rs.getString(colIdx);
					case INT:
						return rs.getInt(colIdx);
					case LONG:
						return rs.getLong(colIdx);
					case DOUBLE:
						return rs.getDouble(colIdx);
					case BINARY:
						return rs.getBytes(colIdx);
					case FLOAT:
						return rs.getFloat(colIdx);
					case DATETIME:
						return rs.getTimestamp(colIdx).toLocalDateTime();
					case BOOLEAN:
						return rs.getBoolean(colIdx);
					case DATE:
						return rs.getDate(colIdx).toLocalDate();
					case TIME:
						return rs.getTime(colIdx).toLocalTime();
					default:
						throw new RecordSetException("unexpected DataType: " + col.type());
				}
			}
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to load column: column=" + col + ", cause=" + e);
		}
	}
	
	private void setColumn(PreparedStatement pstmt, int idx, Column col, Object value)
		throws RecordSetException {
		try {
			if ( col.type().isGeometryType() ) {
				setGeometryColumn(pstmt, idx, col, (Geometry)value);
			}
			else {
				switch ( col.type().getTypeCode() ) {
					case STRING:
						// PostgreSQL의 경우 문자열에 '0x00'가 포함되는 경우
						// 오류를 발생시키기 때문에, 삽입전에 제거시킨다.
						String str = (String)value;
						if ( str != null ) {
							str = str.replaceAll("\\x00","");
						}
						pstmt.setString(idx, str);
						break;
					case INT:
						pstmt.setInt(idx, (Integer)value);
						break;
					case LONG:
						pstmt.setLong(idx, (Long)value);
						break;
					case SHORT:
						pstmt.setShort(idx, (Short)value);
						break;
					case DOUBLE:
						pstmt.setDouble(idx, (Double)value);
						break;
					case FLOAT:
						pstmt.setFloat(idx, (Float)value);
						break;
					case BINARY:
						pstmt.setBytes(idx, (byte[])value);
						break;
					case DATETIME:
						pstmt.setLong(idx, LocalDateTimes.toUtcMillis((LocalDateTime)value));
						break;
					case DATE:
						pstmt.setLong(idx, LocalDates.toUtcMillis((LocalDate)value));
						break;
					case TIME:
						pstmt.setString(idx, LocalTimes.toString((LocalTime)value));
						break;
					case BOOLEAN:
						pstmt.setBoolean(idx, (Boolean)value);
						break;
					default:
						throw new RecordSetException("unexpected DataType: " + col.type());
				}
			}
		}
		catch ( Exception e ) {
			throw new RecordSetException("fails to load column: column=" + col + ", cause=" + e);
		}
	}
	
	private static Class<? extends JdbcRecordAdaptor> getAdaptorClass(String protocol) {
		return KVFStream.from(JDBC_PROCESSORS)
						.filter(kv -> kv.key().equals(protocol))
						.next()
						.map(kv -> kv.value())
						.getOrThrow(() -> new IllegalArgumentException("unsupported Jdbc protocol: " + protocol));

	}
	
	private static final Map<String,Class<? extends JdbcRecordAdaptor>> JDBC_PROCESSORS;
	static {
		JDBC_PROCESSORS = Maps.newHashMap();
		JDBC_PROCESSORS.put("postgresql", PostgreSQLRecordAdaptor.class);
		JDBC_PROCESSORS.put("mysql", MySQLRecordAdaptor.class);
	}
}
