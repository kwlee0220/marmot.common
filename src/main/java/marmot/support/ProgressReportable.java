package marmot.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ProgressReportable {
	public static final Logger s_logger = LoggerFactory.getLogger(ProgressReportable.class);
	
	/**
	 * 현재 수행 상태를 {@link Logger}를 통해 출력한다.
	 * 
	 * @param logger	수행 상태를 출력할 logger 객체.
	 * @param elapsed	지금까지의 경과시간. 일반적으로 레코드 당 처리속도를 계산할 때
	 * 					사용하기 위해 제공됨.
	 */
	public void reportProgress(Logger logger, StopWatch elapsed);
	
	public static void reportProgress(Object reporter, Logger logger, StopWatch elapsed) {
		if ( reporter instanceof ProgressReportable ) {
			((ProgressReportable)reporter).reportProgress(logger, elapsed);
		}
	}
}
