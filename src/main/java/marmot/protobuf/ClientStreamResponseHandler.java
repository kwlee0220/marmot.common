package marmot.protobuf;

import java.util.concurrent.ExecutionException;

import io.grpc.stub.StreamObserver;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.async.CancellableWork;
import utils.async.Result;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClientStreamResponseHandler<T> implements StreamObserver<T> {
	private final CancellableWork m_streamer;
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private Result<T> m_result = null;
	
	public ClientStreamResponseHandler(CancellableWork uploader) {
		m_streamer = uploader;
	}
	
	public T get() throws InterruptedException, ExecutionException {
		return m_guard.awaitUntilAndGet(() -> m_result != null, () -> m_result).get();
	}
	
	public Result<T> waitForResult() throws InterruptedException {
		return m_guard.awaitUntilAndGet(() -> m_result != null, () -> m_result);
	}

	@Override
	public void onNext(T value) {
		m_guard.run(() -> m_result = Result.completed(value), true);
	}

	@Override
	public void onError(Throwable t) {
		m_guard.run(() -> m_result = Result.failed(t), true);
	}

	@Override
	public void onCompleted() {
		m_guard.run(() -> {
			if ( m_result == null ) {
				m_result = Result.completed(null);
			}
			
			m_streamer.cancelWork();
		}, true);
	}
}