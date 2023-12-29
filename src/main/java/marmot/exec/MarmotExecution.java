package marmot.exec;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import utils.func.FOption;

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
	
	/**
	 * 식별자를 반환한다.
	 * 
	 * @return	MarmotExecution 식별자
	 */
	public String getId();
	
	/**
	 * 연산 수행에 관련된 {@link MarmotAnalysis}를 반환한다.
	 * 특정 분석의 수행으로 생성된 수행인 경우, 해당 분석 객체를 반환하고,
	 * 그렇지 않은 경우는 {@link FOption#empty()}를 반환한다.
	 * 
	 * @return	분석의 수행인 경우는 {@link FOption#of(Object)},
	 * 			그렇지 않은 경우는 {@link FOption#empty()}
	 * 
	 */
	public FOption<MarmotAnalysis> getMarmotAnalysis();
	
	/**
	 * 복합 분석으로 생성된 수행인 경우 현재 수행 중인 원소 연산의 순번를 반환한다.
	 * 그렇지 않은 경우는 0을 ㅂ반환한다.
	 * 
	 * @return	연산 순서
	 */
	public default int getCurrentExecutionIndex() {
		return 0;
	}
	
	/**
	 * 연산 수행 상태를 반환한다.
	 * 
	 * @return	연산 수행 상태.
	 */
	public State getState();
	
	/**
	 * 연산의 수행 여부를 반환한다.
	 * 
	 * @return	수행여부
	 */
	public default boolean isRunning() {
		return getState() == State.RUNNING;
	}
	
	/**
	 * 연산의 실패 원인 예외를 반환한다.
	 * 
	 * @return	실패 유발 예외 객체
	 * @throws IllegalStateException	연산이 실패하지 않은 경우
	 */
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
	 * 작업 중단을 확인하기 위해서는 {@link #pollInfinite()}이나 {@link #waitForDone(long, TimeUnit)}
	 * 메소드를 사용하여 최종적으로 확인할 수 있다.
	 * 
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
	
	/**
	 * 연산의 시작 시각을 반환한다.
	 * 
	 * @return	UTC epoch millis
	 */
	public long getStartedTime();
	
	/**
	 * 연산의 종료 시각을 반환한다.
	 * 
	 * @return	UTC epoch millis
	 */
	public long getFinishedTime();
	
	/**
	 * 연산의 최대 수행 시간을 반환한다.
	 * 최대 수행시간이 경과된 수행은 강제로 종료된다.
	 * 
	 * @return	수행 시간
	 */
	public Duration getMaximumRunningTime();
	
	/**
	 * 연산의 최대 수행시간을 설정한다.
	 * 
	 * @param dur	수행기간
	 */
	public void setMaximumRunningTime(Duration dur);
	
	/**
	 * 연산 종료 후 연산 상태를 유지하는 최대 시간을 반환한다.
	 * 연산의 수행 결과와 무관하게 적용되며, 이 기간이 초과되면 
	 * 해당 연산의 정보는 삭제된다.
	 * 
	 * @return	유지기간
	 */
	public Duration getRetentionTime();
	
	/**
	 * 연산 종료 후 연산 상태를 유지하는 최대 시간을 설정한다.
	 * 
	 * @param dur	유지기간
	 */
	public void setRetentionTime(Duration dur);
}
