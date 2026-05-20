package marmot.externio.jdbc;

import utils.Preconditions;

import marmot.RecordSchema;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefaultRecordAdaptor extends JdbcRecordAdaptor {
	public DefaultRecordAdaptor(RecordSchema schema, GeometryFormat format) {
		super(schema, format);
		
		Preconditions.checkArgument(format != GeometryFormat.NATIVE,
							"default JdbcRecordAdaptor does not support 'NATIVE' geometry format");
	}
}
