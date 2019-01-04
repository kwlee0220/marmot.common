package marmot.rset;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Objects;
import java.util.concurrent.CancellationException;

import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.support.DefaultRecord;
import utils.async.AbstractThreadedExecution;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBRecordSetInputStream extends InputStream {
	private static final int DEFAULT_PIPE_SIZE = 64 * 1024;
	
	private final PipedInputStream m_pipe;
	private final RecordSetPump m_pump;
	
	public static PBRecordSetInputStream from(RecordSet rset) {
		return new PBRecordSetInputStream(rset);
	}
	
	public static PBRecordSetInputStream from(RecordSchema schema,
											FStream<Record> rstream) throws IOException {
		return from(RecordSet.from(schema, rstream));
	}
	
	private PBRecordSetInputStream(RecordSet rset) {
		Objects.requireNonNull(rset, "RecordSet");
		
		try {
			PipedOutputStream pipeOut = new PipedOutputStream();
			m_pipe = new PipedInputStream(pipeOut, DEFAULT_PIPE_SIZE);
			
			m_pump = new RecordSetPump(rset, pipeOut);
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
		return m_pipe.read();
	}

	@Override
    public int read(byte b[], int off, int len) throws IOException {
		return m_pipe.read(b, off, len);
	}
	
	private static class RecordSetPump extends AbstractThreadedExecution<Void> {
		private final RecordSet m_rset;
		private final OutputStream m_os;
		
		private RecordSetPump(RecordSet rset, OutputStream os) {
			m_rset = rset;
			m_os = os;
			
			setLogger(LoggerFactory.getLogger(RecordSetPump.class));
		}

		@Override
		protected Void executeWork() throws CancellationException, Exception {
			Record rec = DefaultRecord.of(m_rset.getRecordSchema());
			
			try {
				m_rset.getRecordSchema().toProto().writeDelimitedTo(m_os);
				
				while ( m_rset.next(rec) ) {
					if ( !isRunning() ) {
						return null;
					}
					
					rec.toProto().writeDelimitedTo(m_os);
				}
				getLogger().debug("END-OF-RSET");
			}
			catch ( InterruptedIOException e ) {
				throw new CancellationException("" + e);
			}
			finally {
				IOUtils.closeQuietly(m_os);
			}
			
			return null;
		}
	}
}
