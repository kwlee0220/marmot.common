package marmot.externio.jdbc;

import marmot.RecordSchema;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefaultRecordAdaptor extends JdbcRecordAdaptor {
	public DefaultRecordAdaptor(RecordSchema schema, GeometryFormat format) {
		super(schema, format);
		
		Utilities.checkArgument(format != GeometryFormat.NATIVE,
							"default JdbcRecordAdaptor does not support 'NATIVE' geometry format");
	}
}
