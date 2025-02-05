package marmot.protobuf;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.stub.StreamObserver;

import utils.async.Guard;
import utils.async.GuardedSupplier;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SingleValueObserver<T> implements StreamObserver<T> {
	private static final Logger s_logger = LoggerFactory.getLogger(SingleValueObserver.class);
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private boolean m_done = false;
	@GuardedBy("m_guard") private T m_value;
	@GuardedBy("m_guard") private Throwable m_cause;
	
	public static <T> SingleValueObserver<T> create() {
		return new SingleValueObserver<>();
	}
	
	public void await() throws InterruptedException {
		m_guard.awaitUntil(() -> m_done);
	}
	
	public T get() {
		return GuardedSupplier.from(m_guard, () -> {
			if ( m_cause != null ) {
				throw m_cause;
			} else {
				return m_value;
			}
		})
		.preCondition(() -> m_done)
		.get();
	}

	@Override
	public void onNext(T value) {
		m_guard.run(() -> m_value = value);
	}

	@Override
	public void onError(Throwable error) {
		s_logger.error("unexpected onError: class={}, error={}", getClass(), error);
		
		m_guard.runAndSignalAll(() -> {
			m_cause = error;
			m_done = true;
		});
	}

	@Override
	public void onCompleted() {
		m_guard.runAndSignalAll(() -> m_done = true);
	}
}
