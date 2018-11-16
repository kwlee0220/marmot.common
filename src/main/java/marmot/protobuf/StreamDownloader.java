package marmot.protobuf;

import java.io.InputStream;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import utils.async.AbstractExecution;
import utils.async.CancellableWork;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class StreamDownloader<MSG> extends AbstractExecution<Void>
											implements CancellableWork {
	private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
	
	private final InputStream m_is;
	private StreamObserver<MSG> m_channel = null;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;
	
	protected abstract MSG wrapChunk(ByteString chunk);
	
	public StreamDownloader(InputStream is) {
		Objects.requireNonNull(is, "InputStream");
		
		m_is = is;
	}
	
	public StreamDownloader<MSG> channel(StreamObserver<MSG> channel) {
		Objects.requireNonNull(channel, "Uploading channel");
		
		m_channel = channel;
		return this;
	}
	
	public StreamDownloader<MSG> chunkSize(int size) {
		Preconditions.checkArgument(size > 0, "chunkSize > 0");
		
		m_chunkSize = size;
		return this;
	}

	@Override
	public Void executeWork() throws Exception {
		Preconditions.checkState(m_channel != null, "Upload channel has not been set");

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
			}
			
			return null;
		}
		catch ( Exception e ) {
			throw e;
		}
		finally {
			IOUtils.closeQuietly(m_is);
		}
	}

	@Override
	public boolean cancelWork() {
		return true;
	}
}
