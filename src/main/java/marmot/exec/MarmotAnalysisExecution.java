package marmot.exec;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface MarmotAnalysisExecution extends MarmotExecution {
	public MarmotAnalysis getMarmotAnalysis();
	
	/**
	 * 복합 연산의 경우 현재 수행 중인 원소 연산의 순번를 반환한다.
	 * 복합 연산이 아닌 경우는 0을 ㅂ반환한다.
	 * 
	 * @return	연산 순서
	 */
	public default int getCurrentExecutionIndex() {
		return 0;
	}
}
