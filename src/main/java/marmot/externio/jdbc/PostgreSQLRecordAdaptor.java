package marmot.externio.jdbc;

import marmot.Column;
import marmot.RecordSchema;
import marmot.type.DataType;
import marmot.type.TypeCode;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PostgreSQLRecordAdaptor extends JdbcRecordAdaptor {
	public static final String JDBC_DRIVER_CLASS = "org.postgresql.Driver";
	
	public PostgreSQLRecordAdaptor(RecordSchema schema) {
		super(schema);
	}

	@Override
	protected String declareSQLColumn(Column col) {
		DataType type = col.type();
		if ( type.isGeometryType() ) {
			return String.format("%s bytea", col.name());
		}
		else if ( type.getTypeCode() == TypeCode.BINARY || type.getTypeCode() == TypeCode.TYPED ) {
			return String.format("%s bytea", col.name());
		}
		else {
			return super.declareSQLColumn(col);
		}
	}
}
