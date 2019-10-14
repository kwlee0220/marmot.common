package marmot.exec;

import static marmot.support.DateTimeFunctions.DateTimeFromMillis;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractMarmotExecution implements MarmotExecution  {
	private final String m_id;
	protected final long m_startedTime;
	
	protected volatile long m_finishedTime = -1;
	private volatile Duration m_maxRunningTime = Duration.ofDays(1);
	private volatile Duration m_retentionTime = Duration.ofDays(1);
	
	protected AbstractMarmotExecution(String id) {
		m_id = id;
		m_startedTime = System.currentTimeMillis();
	}
	
	protected AbstractMarmotExecution() {
		m_id = "" + System.identityHashCode(this);
		m_startedTime = System.currentTimeMillis();
	}

	@Override
	public String getId() {
		return m_id;
	}

	@Override
	public long getStartedTime() {
		return m_startedTime;
	}

	@Override
	public long getFinishedTime() {
		return m_finishedTime;
	}

	@Override
	public Duration getMaximumRunningTime() {
		return m_maxRunningTime;
	}

	@Override
	public void setMaximumRunningTime(Duration dur) {
		m_maxRunningTime = dur;
	}

	@Override
	public Duration getRetentionTime() {
		return m_retentionTime;
	}

	@Override
	public void setRetentionTime(Duration dur) {
		m_retentionTime = dur;
	}
	
	@Override
	public String toString() {
		String failedCause = "";
		if ( getState() == State.FAILED ) {
			failedCause = String.format(" (cause=%s)", getFailureCause());
		}
		
		LocalDateTime ldt = DateTimeFromMillis(m_startedTime);
		String finishedStr = "";
		if ( m_finishedTime > 0 ) {
			finishedStr = String.format(", finished=%s", DateTimeFromMillis(m_finishedTime));
		}
		
		return String.format("%s: %s%s, started=%s%s", getId(), getState(), failedCause,
								ldt, finishedStr);
	}
}
