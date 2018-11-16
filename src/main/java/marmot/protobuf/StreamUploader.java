package marmot.protobuf;

import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import io.vavr.control.Option;
import marmot.proto.service.StreamUploadResponse;
import net.jcip.annotations.GuardedBy;
import utils.Guard;
import utils.async.AbstractExecution;
import utils.async.CancellableWork;
import utils.async.Result;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class StreamUploader<REQ> extends AbstractExecution<Void>
								implements CancellableWork, StreamObserver<StreamUploadResponse> {
	private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
	private static final int SYNC_INTERVAL = 4;
	
	private final InputStream m_is;
	private StreamObserver<REQ> m_channel = null;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private Result<Long> m_result = null;
	@GuardedBy("m_guard") private int m_sync = 0;
	
	protected abstract Option<REQ> newHeader();
	protected abstract REQ wrapChunk(ByteString chunk);
	protected abstract REQ wrapSync(int sync);
	
	public StreamUploader(InputStream is) {
		Objects.requireNonNull(is, "InputStream");
		
		m_is = is;
	}
	
	public StreamUploader<REQ> channel(StreamObserver<REQ> channel) {
		Objects.requireNonNull(channel, "Uploading channel");
		
		m_channel = channel;
		return this;
	}
	
	public StreamUploader<REQ> chunkSize(int size) {
		Preconditions.checkArgument(size > 0, "chunkSize > 0");
		
		m_chunkSize = size;
		return this;
	}

	@Override
	public Void executeWork() throws Exception {
		Preconditions.checkState(m_channel != null, "Upload channel has not been set");
		
		newHeader().forEach(m_channel::onNext);

		int blockCount = 0;
		try {
			while ( true ) {
				if ( isCancelRequested() ) {
					throw new InterruptedException("interrupted by user");
				}
				
				LimitedInputStream chunk = new LimitedInputStream(m_is, m_chunkSize);
				ByteString bstring = ByteString.readFrom(chunk);
				if ( bstring.isEmpty() ) {
					break;
				}
				
				m_channel.onNext(wrapChunk(bstring));
				if ( (++blockCount % SYNC_INTERVAL) == 0 ) {
					m_channel.onNext(wrapSync(blockCount));
					
					waitForSync(blockCount - SYNC_INTERVAL);
				}
			}
			
			ByteString endMarker = ByteString.EMPTY;
			m_channel.onNext(wrapChunk(endMarker));
			
			return null;
		}
		catch ( Exception e ) {
System.out.println("XXXXXXXXXXXXXXX");
e.printStackTrace(System.err);
			throw e;
		}
	}

	@Override
	public boolean cancelWork() {
		return true;
	}
	
	public long waitForFinalReponse() throws InterruptedException, ExecutionException {
		return m_guard.awaitUntilAndGet(() -> m_result != null, () -> m_result).get();
	}
	
	private void waitForSync(int sync) throws InterruptedException {
		m_guard.awaitUntil(() -> m_sync >= sync);
	}

	@Override
	public void onNext(StreamUploadResponse resp) {
		switch ( resp.getEitherCase() ) {
			case SYNC_BACK:
				m_guard.runAndSignal(() -> m_sync = resp.getSyncBack());
				break;
			case RESULT:
				m_guard.runAndSignal(() -> m_result = Result.completed(resp.getResult()));
				break;
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable t) {
		m_guard.runAndSignal(() -> m_result = Result.failed(t));
	}

	@Override
	public void onCompleted() {
		m_guard.runAndSignal(() -> {
			if ( m_result == null ) {
				m_result = Result.completed(null);
			}
			
			StreamUploader.this.cancelWork();
		});
	}
}
