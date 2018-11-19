package marmot.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

import marmot.remote.protobuf.PBStreamClosedException;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.Throwables;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ChunkInputStream extends InputStream {
	private static final Logger s_logger = LoggerFactory.getLogger(ChunkInputStream.class);
	
	private final int m_capacity;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private final List<ByteString> m_chunkQueue;
	@GuardedBy("m_guard") private int m_chunkCount = 0;
	@GuardedBy("m_guard") private boolean m_closed = false;
	@GuardedBy("m_guard") private boolean m_eos = false;
	@GuardedBy("m_guard") private Throwable m_cause = null;
	private ByteString m_buffer;
	private int m_offset;
	private long m_totalOffset = 0;
	
	public static ChunkInputStream create(int capacity) {
		return new ChunkInputStream(capacity);
	}
	
	private ChunkInputStream(int capacity) {
		m_capacity = capacity;
		m_chunkQueue = Lists.newArrayListWithCapacity(capacity);
		m_buffer = null;
	}
	
	@Override
	public void close() throws IOException {
		m_guard.run(() -> {
			if ( !m_eos ) {
				s_logger.info("{} is closed abruptly", getClass().getSimpleName());
			}
			
			m_chunkQueue.clear();
			m_closed = true;
		}, true);
		
		super.close();
	}
	
	public boolean isClosed() {
		return m_guard.get(() -> m_closed);
	}
	
	public long offset() {
		return m_totalOffset;
	}

	@Override
	public int read() throws IOException {
		locateCurrentChunk();
		if ( m_buffer != null ) {
			++m_totalOffset;
			return m_buffer.byteAt(m_offset++) & 0x000000ff;
		}
		else {
			return -1;
		}
	}

	@Override
    public int read(byte b[], int off, int len) throws IOException {
		locateCurrentChunk();
		if ( m_buffer != null ) {
			int nbytes = Math.min(m_buffer.size()-m_offset, len);
			m_buffer.copyTo(b, m_offset, off, nbytes);
			m_offset += nbytes;
			m_totalOffset += nbytes;

			return nbytes;
		}
		else {
			return -1;
		}
    }
	
	public void supply(ByteString chunk) throws InterruptedException, PBStreamClosedException {
		final Lock lock = m_guard.getLock();
		final Condition cond = m_guard.getCondition();
		
		lock.lock();
		try {
			while ( true ) {
				if ( m_closed ) {
					throw new PBStreamClosedException("Stream is closed already");
				}
				if ( m_eos ) {
					return;
				}
				if ( m_chunkQueue.size() < m_capacity ) {
					m_chunkQueue.add(chunk);
					++m_chunkCount;
					cond.signalAll();
					
					return;
				}
				
				cond.await();
			}
		}
		finally {
			lock.unlock();
		}
	}
	
	public void endOfSupply() {
		m_guard.run(() -> m_eos = true, true);
	}
	
	public void endOfSupply(Throwable cause) {
		m_guard.run(() -> { m_eos = true; m_cause = cause; }, true);
	}
	
	@Override
	public String toString() {
		return String.format("%s[offset=%d, %s, %s]", getClass().getSimpleName(),
								m_totalOffset, m_closed ? "closed" : "open",
								m_eos ? "eos" : "supplying");
	}
	
	private void locateCurrentChunk() throws IOException {
		try {
			if ( m_buffer == null || (m_offset == m_buffer.size()) ) {
				m_offset = 0;
				m_buffer = getNextChunk();
			}
		}
		catch ( InterruptedException e ) {
			throw new IOException("" + e);
		}
	}
	
	private ByteString getNextChunk() throws InterruptedException, IOException {
		m_guard.getLock().lock();
		try {
			while ( m_chunkQueue.isEmpty() ) {
				if ( m_closed ) {
					throw new IOException("Stream has been closed already");
				}
				if ( m_eos ) {
					// 더 이상의  chunk supply가 없는 경우, 정상적으로 종료된 것인지
					// 오류에 의한 종료인지를 확인한다.
					if ( m_cause != null ) {
						Throwables.throwIfInstanceOf(m_cause, IOException.class);
						throw Throwables.toRuntimeException(m_cause);
					}
					return null;
				}
				
				m_guard.getCondition().await();
			}
			
			m_guard.getCondition().signalAll();
			return m_chunkQueue.remove(0);
		}
		finally {
			m_guard.getLock().unlock();
		}
	}
}