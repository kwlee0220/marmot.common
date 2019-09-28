package marmot.exec;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotExecution {
	public enum State {
		/** 연산이 동작 중인 상태 */
		RUNNING(0),
		/** 연산 수행이 성공적으로 종료된 상태. */
		COMPLETED(1),
		/** 연산 수행 중 오류 발생으로 종료된 상태. */
		FAILED(2),
		/** 연산 수행 중간에 강제로 중단된 상태. */
		CANCELLED(3);
		
		int m_code;
		
		State(int code) {
			m_code = code;
		}
		
		public int getCode() {
			return m_code;
		}
		
		public static State fromCode(int code) {
			return values()[code];
		}
	};
	
	public String getId();
	
	/**
	 * 연산 수행 상태를 반환한다.
	 * 
	 * @return	연산 수행 상태.
	 */
	public State getState();
	
	public default boolean isRunning() {
		return getState() == State.RUNNING;
	}
	
	public Throwable getFailureCause() throws IllegalStateException;
	
	/**
	 * 연산 수행을 중단시킨다.
	 * <p>
	 * 메소드 호출은 연산이 완전히 중단되기 전에 반환될 수 있기 때문에, 본 메소드 호출한 결과로
	 * 바로 종료되는 것을 의미하지 않는다.
	 * 또한 메소드 호출 당시 작업 상태에 따라 중단 요청을 무시되기도 한다.
	 * 이는 본 메소드의 반환값이 {@code false}인 경우는 요청이 명시적으로 무시된 것을 의미한다.
	 * 반환 값이 {@code true}인 경우는 중단 요청이 접수되어 중단 작업이 시작된 것을 의미한다.
	 * 물론, 이때도 중단이 반드시 성공하는 것을 의미하지 않는다.
	 * 
	 * 작업 중단을 확인하기 위해서는 {@link #waitForDone()}이나 {@link #waitForDone(long, TimeUnit)}
	 * 메소드를 사용하여 최종적으로 확인할 수 있다.
	 * 
	 * @param mayInterruptIfRunning	 이미 동적 중인 경우에도 cancel할지 여부
	 * @return	중단 요청의 접수 여부.
	 */
	public boolean cancel();
	
	/**
	 * 작업이 종료될 때까지 대기한다.
	 * 
	 * @throws InterruptedException	작업 종료 대기 중 대기 쓰레드가 interrupt된 경우.
	 */
	public void waitForFinished() throws InterruptedException;

	/**
	 * 본 작업이 종료될 때까지 제한된 시간 동안만 대기한다.
	 * 
	 * @param timeout	대기시간
	 * @param unit		대기시간 단위
	 * @return	제한시간 전에 성공적으로 반환하는 경우는 {@code true},
	 * 			대기 중에 제한시간 경과로 반환되는 경우는 {@code false}.
	 * @throws InterruptedException	작업 종료 대기 중 대기 쓰레드가 interrupt된 경우.
	 */
	public boolean waitForFinished(long timeout, TimeUnit unit) throws InterruptedException;
	
	public long getStartedTime();
	public long getFinishedTime();
	
	public Duration getMaximumRunningTime();
	public void setMaximumRunningTime(Duration dur);
	
	public Duration getRetentionTime();
	public void setRetentionTime(Duration dur);
}
