package marmot.protobuf;

import java.io.IOException;
import java.io.InputStream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSetException;
import marmot.proto.RecordProto;
import marmot.proto.RecordSchemaProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import utils.Throwables;
import utils.Utilities;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBInputStreamRecordSet extends AbstractRecordSet {
	private final RecordSchema m_schema;
	private final InputStream m_is;
	
	public static PBInputStreamRecordSet from(InputStream is) {
		return new PBInputStreamRecordSet(is);
	}
	
	private PBInputStreamRecordSet(InputStream is) {
		Utilities.checkNotNullArgument(is, "InputStream");
		
		try {
			m_schema = RecordSchema.fromProto(RecordSchemaProto.parseDelimitedFrom(is));
			m_is = is;
		}
		catch ( Exception e ) {
			Throwables.throwIfInstanceOf(e, RuntimeException.class);
			throw new RecordSetException(e);
		}
	}

	@Override
	protected void closeInGuard() {
		IOUtils.closeQuietly(m_is);
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}
	
	@Override
	public boolean next(Record output) {
		try {
			RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
			if ( proto != null ) {
				PBRecordProtos.fromProto(proto, output);
				return true;
			}
			else {
				return false;
			}
		}
		catch ( IOException e ) {
			throw new RecordSetException("" + e);
		}
	}
	
	@Override
	public Record nextCopy() {
		try {
			RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
			if ( proto != null ) {
				return DefaultRecord.fromProto(m_schema, proto);
			}
			else {
				return null;
			}
		}
		catch ( IOException e ) {
			throw new RecordSetException("" + e);
		}
	}
}
