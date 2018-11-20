package marmot.rset;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.support.DefaultRecord;
import utils.async.AbstractExecution;
import utils.async.CancellableWork;
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
		return from(RecordSets.from(schema, rstream));
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
		m_pump.cancel();
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
	
	private static class RecordSetPump extends AbstractExecution<Void>
										implements CancellableWork {
		private final RecordSet m_rset;
		private final OutputStream m_os;
		private final AtomicBoolean m_cancelRequested = new AtomicBoolean(false);
		
		private RecordSetPump(RecordSet rset, OutputStream os) {
			m_rset = rset;
			m_os = os;
			
			setLogger(LoggerFactory.getLogger(RecordSetPump.class));
		}

		@Override
		public Void executeWork() throws CancellationException, Throwable {
			Record rec = DefaultRecord.of(m_rset.getRecordSchema());
			
			try {
				m_rset.getRecordSchema().toProto().writeDelimitedTo(m_os);
				
				while ( m_rset.next(rec) ) {
					rec.toProto().writeDelimitedTo(m_os);
					
					if ( m_cancelRequested.get() ) {
						throw new InterruptedException();
					}
				}
				
				getLogger().debug("END-OF-RSET");
			}
			catch ( IOException e ) {
				if ( isCancelRequested() ) {
					throw new InterruptedException();
				}
				
				throw e;
			}
			finally {
				IOUtils.closeQuietly(m_os);
			}
			
			return null;
		}

		@Override
		public boolean cancelWork() {
			m_cancelRequested.set(true);
			IOUtils.closeQuietly(m_os);
			
			return true;
		}
		
	}
}
