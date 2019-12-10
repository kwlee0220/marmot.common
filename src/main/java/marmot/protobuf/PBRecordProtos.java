package marmot.protobuf;

import static utils.Utilities.checkNotNullArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CancellationException;

import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.proto.RecordProto;
import marmot.proto.RecordSchemaProto;
import marmot.proto.ValueProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import utils.Throwables;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.async.StartableExecution;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBRecordProtos {
	private PBRecordProtos() {
		throw new AssertionError("Should not be called: " + getClass());
	}

	/**
	 * ProtoBuf 방식으로 인코딩된 주어진 입력 스트림에 저장된 레코드세트를 읽는다.
	 * 
	 *  @param is		ProtoBuf 형식으로 인코딩된 입력 스트림. 
	 *  @return	레코드 세트
	 */
	public static RecordSet readRecordSet(InputStream is) {
		return new PBInputStreamRecordSet(is);
	}
	static class PBInputStreamRecordSet extends AbstractRecordSet {
		private final RecordSchema m_schema;
		private final InputStream m_is;
		
		private PBInputStreamRecordSet(InputStream is) {
			checkNotNullArgument(is, "InputStream");
			
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
		protected void closeInGuard() { }

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
	
	public static StartableExecution<Long> newWriteExecution(RecordSet rset, OutputStream os) {
		return new WriteRecordSetToOutStream(rset, os);
	}
	private static class WriteRecordSetToOutStream extends AbstractThreadedExecution<Long> {
		private final RecordSet m_rset;
		private final OutputStream m_os;
		
		private WriteRecordSetToOutStream(RecordSet rset, OutputStream os) {
			m_rset = rset;
			m_os = os;
			
			setLogger(LoggerFactory.getLogger(WriteRecordSetToOutStream.class));
		}

		@Override
		protected Long executeWork() throws CancellationException, Exception {
			Record rec = DefaultRecord.of(m_rset.getRecordSchema());
			
			long count = 0;
			try {
				m_rset.getRecordSchema().toProto().writeDelimitedTo(m_os);
				while ( m_rset.next(rec) ) {
					if ( !isRunning() ) {
						break;
					}
					
					PBRecordProtos.toProto(rec).writeDelimitedTo(m_os);
					++count;
				}
				
				return count;
			}
			catch ( InterruptedIOException e ) {
				throw new CancellationException("" + e);
			}
			finally {
				m_rset.closeQuietly();
				IOUtils.closeQuietly(m_os);
			}
		}
	}
	
	public static InputStream toInputStream(RecordSet rset) {
		return new PBRecordSetInputStream(rset);
	}
	public static InputStream toInputStream(RecordSchema schema, FStream<Record> rstream) {
		return toInputStream(RecordSet.from(schema, rstream));
	}
	private static class PBRecordSetInputStream extends InputStream {
		private static final int DEFAULT_PIPE_SIZE = 64 * 1024;
		
		private final PipedInputStream m_pipe;
		private final StartableExecution<Long> m_pump;
		private Throwable m_error;
		
		private PBRecordSetInputStream(RecordSet rset) {
			Utilities.checkNotNullArgument(rset, "RecordSet");
			
			try {
				PipedOutputStream pipeOut = new PipedOutputStream();
				m_pipe = new PipedInputStream(pipeOut, DEFAULT_PIPE_SIZE);
				
				m_pump = PBRecordProtos.newWriteExecution(rset, pipeOut);
				m_pump.whenFailed(error -> m_error = error);
				m_pump.start();
			}
			catch ( IOException e ) {
				throw new RecordSetException(e);
			}
		}
		
		@Override
		public void close() throws IOException {
			m_pump.cancel(true);
			m_pipe.close();
		}

		@Override
		public int read() throws IOException {
			int ret = m_pipe.read();
			if ( ret >= 0 || m_error == null ) {
				return ret;
			}
			
			throw throwException(m_error);
		}

		@Override
	    public int read(byte b[], int off, int len) throws IOException {
			int ret = m_pipe.read(b, off, len);
			if ( ret >= 0 || m_error == null ) {
				return ret;
			}

			throw throwException(m_error);
		}
		
		private static IOException throwException(Throwable error) {
			if ( error instanceof IOException ) {
				return (IOException)error;
			}
			Throwables.throwIfInstanceOf(error, RecordSetException.class);
			throw new RecordSetException("" + error);
		}
	}
	
	public static RecordProto toProto(Record record) {
		return toProto(record.getRecordSchema(), record.getAll());
	}

	public static RecordProto toProto(RecordSchema schema, Object[] values) {
		RecordProto.Builder builder = RecordProto.newBuilder();
		
		for ( int i =0; i < values.length; ++i ) {
			Column col = schema.getColumnAt(i);
			
			ValueProto vproto = PBValueProtos.toValueProto(col.type().getTypeCode(), values[i]);
			builder.addColumn(vproto);
		}
		
		return builder.build();
	}
	
	public static RecordProto toProto(Object[] values) {
		return FStream.of(values)
						.map(PBValueProtos::toValueProto)
						.foldLeft(RecordProto.newBuilder(), (b,p) -> b.addColumn(p))
						.build();
	}
	
	public static void fromProto(RecordProto proto, RecordSchema schema, Object[] values) {
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			values[i] = PBValueProtos.fromProto(proto.getColumn(i));
		}
	}
	
	public static void fromProto(RecordProto proto, Record record) {
		for ( int i =0; i < record.getColumnCount(); ++i ) {
			record.set(i, PBValueProtos.fromProto(proto.getColumn(i)));
		}
	}
	
	public static Record fromProto(RecordProto proto, RecordSchema schema) {
		Record record = DefaultRecord.of(schema);
		fromProto(proto, record);
		return record;
	}
}
