package marmot.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.google.protobuf.ByteString;

import io.vavr.control.Option;
import io.vavr.control.Try;
import marmot.proto.service.StreamChunkProto;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ChunkInputStream extends InputStream {
	private final FStream<Try<ByteString>> m_chunks;
	private byte[] m_buffer;
	private int m_offset;
	
	public static ChunkInputStream from(FStream<Try<ByteString>> chunks) {
		return new ChunkInputStream(chunks);
	}
	
	public static ChunkInputStream fromSuccessful(FStream<ByteString> chunks) {
		return new ChunkInputStream(chunks.map(Try::success));
	}
	
	public static ChunkInputStream from(Iterator<StreamChunkProto> chunks) {
		return fromSuccessful(FStream.of(chunks).map(StreamChunkProto::getBlock));
	}
	
	private ChunkInputStream(FStream<Try<ByteString>> chunks) {
		m_chunks = chunks;
		m_buffer = null;
	}

	@Override
	public int read() throws IOException {
		if ( m_buffer == null || (m_offset == m_buffer.length) ) {
			m_buffer = getNextChunk();
			if ( m_buffer == null ) {
				return -1;
			}
			
			m_offset = 0;
		}

		return m_buffer[m_offset++] & 0x000000ff;
	}

	@Override
    public int read(byte b[], int off, int len) throws IOException {
		if ( m_buffer == null || (m_offset == m_buffer.length) ) {
			m_buffer = getNextChunk();
			if ( m_buffer == null ) {
				return -1;
			}
			
			m_offset = 0;
		}
		
		int nbytes = Math.min(m_buffer.length-m_offset, len);
		System.arraycopy(m_buffer, m_offset, b, off, nbytes);
		m_offset += nbytes;

		return nbytes;
    }
	
	private byte[] getNextChunk() throws IOException {
		Option<Try<ByteString>> chunk = m_chunks.next();
		return (chunk.isDefined()) ? chunk.get().get().toByteArray() : null;
	}
}