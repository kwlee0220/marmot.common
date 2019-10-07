package marmot.exec;

import java.time.Duration;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractMarmotExecution implements MarmotExecution  {
	private final String m_id;
	private final long m_startedTime;
	
	protected volatile long m_finishedTime = -1;
	private volatile Duration m_maxRunningTime = null;
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
	public int getWorkingExecutionIndex() {
		return 0;
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
}
