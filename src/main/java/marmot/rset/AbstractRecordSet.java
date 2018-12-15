package marmot.rset;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSet;
import marmot.RecordSetClosedException;
import marmot.support.DefaultRecord;
import utils.LoggerSettable;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordSet implements RecordSet, LoggerSettable {
	private Logger m_logger = LoggerFactory.getLogger(getClass());
	
	private AtomicBoolean m_closed = new AtomicBoolean(false);
	
	protected abstract void closeInGuard();
	
	public boolean isClosed() {
		return m_closed.get();
	}

	@Override
	public final void close() {
		if ( m_closed.compareAndSet(false, true) ) {
			try {
				closeInGuard();
				m_closed.set(true);
			}
			catch ( Throwable e ) {
				getLogger().warn("fails to close RecordSet: " + this
								+ ", cause=" + Throwables.unwrapThrowable(e));
			}
		}
	}
	
	public boolean next(Record output) {
		Objects.requireNonNull(output, "output Record");
		
		Record next = nextCopy();
		if ( next != null ) {
			output.set(next, false);
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public Record nextCopy() {
		Record output = DefaultRecord.of(getRecordSchema());
		return ( next(output) ) ? output : null;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}
	
	protected void checkNotClosed() {
		if ( isClosed() ) {
			throw new RecordSetClosedException("already closed: this=" + getClass());
		}
	}
}
