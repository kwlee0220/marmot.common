package marmot.remote.protobuf;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.ColumnNotFoundException;
import marmot.DataSetExistsException;
import marmot.DataSetNotFoundException;
import marmot.MarmotInternalException;
import marmot.PlanExecutionException;
import marmot.RecordSetClosedException;
import marmot.RecordSetException;
import marmot.optor.RecordSetOperatorException;
import marmot.support.PBException;
import utils.Throwables;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum PBMarmotError {
	DATASET_NOT_FOUND(0, DataSetNotFoundException.class),
	DATASET_EXISTS(1, DataSetExistsException.class),

	STREAM_CLOSED(10, PBStreamClosedException.class),
	RECORD_SET_CLOSED(15, RecordSetClosedException.class),
	RECORD_SET_ERROR(16, RecordSetException.class),
	
	COLUMN_NOT_FOUND(20, ColumnNotFoundException.class),
	
	PLAN_EXECUTION_INTERRUPTED(101, InterruptedException.class),
	PLAN_EXECUTION_CANCELLED(102, CancellationException.class),
	PLAN_EXECUTION_FAILED(103, PlanExecutionException.class),
	PLAN_EXECUTION_TIMED_OUT(104, TimeoutException.class),
	OPERATOR(110, RecordSetOperatorException.class),

	NULL_POINTER(900, NullPointerException.class),
	INVALID_ARGUMENT(901, IllegalArgumentException.class),
	CANCELLED(999, CancellationException.class),
	INTERAL_ERROR(1000, MarmotInternalException.class),
	;

	private int m_code;
	private Class<? extends Exception> m_class;

	private static final Logger s_logger = LoggerFactory.getLogger(PBMarmotError.class);
	private static final Map<Integer,PBMarmotError> CODE_TO_ERROR;
	static {
		CODE_TO_ERROR = Maps.newHashMap();
		FStream.of(values())
				.forEach(error -> CODE_TO_ERROR.put(error.getCode(), error));
	}
	
	private PBMarmotError(int code, Class<? extends Exception> cls) {
		m_code = code;
		m_class = cls;
	}
	
	public final Class<? extends Exception> getExceptionClass() {
		return m_class;
	}
	
	public final int getCode() {
		return m_code;
	}
	
	public static PBMarmotError fromCode(int code) {
		PBMarmotError error = CODE_TO_ERROR.get(code);
		if ( error == null ) {
			s_logger.warn("unregistered error message: code=" + code);
			throw new IllegalArgumentException("invalid error code=" + code);
		}
		
		return error;
	}
	
	public Exception toException(FOption<String> details) {
		try {
			if ( details.isPresent() ) {
				Constructor<? extends Exception> ctor = m_class.getConstructor(String.class);
				return ctor.newInstance(details.get());
			}
			else {
				return m_class.newInstance();
			}
		}
		catch ( Exception e ) {
			throw new RuntimeException(Throwables.unwrapThrowable(e));
		}
	}
	
	public RuntimeException toRuntimeException(FOption<String> details) {
		Exception ex = toException(details);
		if ( ex instanceof RuntimeException ) {
			return (RuntimeException)ex;
		}
		else {
			throw new PBException("not RuntimeException: exception=" + m_class);
		}
	}
	
	public static PBMarmotError fromClass(Class<?> cls) {
		return FStream.of(values())
					.filter(error -> error.getExceptionClass().equals(cls))
					.next()
					.getOrNull();
	}
}
