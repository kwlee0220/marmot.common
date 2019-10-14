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
import marmot.InsufficientThumbnailException;
import marmot.MarmotInternalException;
import marmot.RecordSetClosedException;
import marmot.RecordSetException;
import marmot.ThumbnailNotFoundException;
import marmot.exec.AnalysisExistsException;
import marmot.exec.AnalysisNotFoundException;
import marmot.exec.ExecutionNotFoundException;
import marmot.exec.MarmotAnalysisException;
import marmot.exec.MarmotExecutionException;
import marmot.geo.catalog.IndexNotFoundException;
import marmot.io.MarmotFileExistsException;
import marmot.io.MarmotFileNotFoundException;
import marmot.proto.service.MarmotErrorCode;
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
	THUMBNAIL(2, InsufficientThumbnailException.class),
	THUMBNAIL_NOT_FOUND(3, ThumbnailNotFoundException.class),
	MARMOT_NOT_FOUND(4, MarmotFileNotFoundException.class),
	MARMOT_EXISTS(5, MarmotFileExistsException.class),
	
	STREAM_CLOSED(50, PBStreamClosedException.class),
	RECORD_SET_CLOSED(55, RecordSetClosedException.class),
	RECORD_SET_ERROR(56, RecordSetException.class),
	
	COLUMN_NOT_FOUND(60, ColumnNotFoundException.class),
	
	INDEX_NOT_FOUND(70, IndexNotFoundException.class),
	
	ERROR_ANALYSIS(MarmotErrorCode.ERROR_ANALYSIS.getNumber(), MarmotAnalysisException.class),
	ERROR_ANALYSIS_NOT_FOUND(MarmotErrorCode.ERROR_ANALYSIS_NOT_FOUND.getNumber(), AnalysisNotFoundException.class),
	ERROR_ANALYSIS_EXISTS(MarmotErrorCode.ERROR_ANALYSIS_EXISTS.getNumber(), AnalysisExistsException.class),

	EXEC_UNKNOWN_ID(101, ExecutionNotFoundException.class),
	EXEC_INTERRUPTED(102, InterruptedException.class),
	EXEC_CANCELLED(103, CancellationException.class),
	EXEC_FAILED(104, MarmotExecutionException.class),
	EXEC_TIMED_OUT(105, TimeoutException.class),

	NULL_POINTER(900, NullPointerException.class),
	INVALID_ARGUMENT(901, IllegalArgumentException.class),
	REMOTE(902, PBRemoteException.class),
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
