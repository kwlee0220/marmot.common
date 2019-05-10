package marmot.command;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotRuntime;
import utils.CommandLine;
import utils.StopWatch;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.io.FileUtils;
import utils.io.IOUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UploadFiles {
	private static final Logger s_logger = LoggerFactory.getLogger(UploadFiles.class);
	
	private final MarmotRuntime m_marmot;
	private final File m_start;
	private PathMatcher m_pathMatcher;
	private final String m_dest;
	private FOption<Long> m_blockSize = FOption.empty();
	private boolean m_force = false;
	
	public static void run(MarmotRuntime marmot, CommandLine cl) throws Exception {
		if ( cl.hasOption("h") ) {
			cl.exitWithUsage(0);
		}
		
		File start = new File(cl.getArgument(0));
		String dest = cl.getArgument(1);
		FOption<Long> blockSize = cl.getOptionString("block_size")
									.map(UnitUtils::parseByteSize);
		String glob = cl.getOptionString("glob").getOrNull();
		boolean force = cl.hasOption("f");
		
		new UploadFiles(marmot, start, dest)
			.glob(glob)
			.blockSize(blockSize)
			.force(force)
			.run();
	}
	
	public UploadFiles(MarmotRuntime marmot, File start, String dest) {
		Utilities.checkNotNullArgument(marmot, "marmot is null");
		Utilities.checkNotNullArgument(start, "source file(or directory)");
		Utilities.checkNotNullArgument(dest, "destination directory path");
		
		m_marmot = marmot;
		m_start = start;
		m_dest = dest;
	}
	
	public UploadFiles glob(String glob) {
		m_pathMatcher = (glob != null)
						? FileSystems.getDefault().getPathMatcher("glob:" + glob)
						: null;
		return this;
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}
	
	public UploadFiles blockSize(long size) {
		m_blockSize = (size > 0) ? FOption.of(size) : FOption.empty();
		return this;
	}
	
	public UploadFiles blockSize(FOption<Long> size) {
		m_blockSize = size;
		return this;
	}
	
	public boolean force() {
		return m_force;
	}
	
	public UploadFiles force(boolean force) {
		m_force = force;
		return this;
	}
	
	public void run() throws Exception {
		StopWatch watch = StopWatch.start();
		
		if ( m_force ) {
			m_marmot.deleteHdfsFile(m_dest);
		}
		
		String prefix = m_start.toPath().toAbsolutePath().toString();
		int prefixLen = prefix.length();
		
		FStream<Path> pathes = FileUtils.walk(m_start.toPath())
										.drop(1);	// root 자신을 제외시킴
		if ( m_pathMatcher != null ) {
			pathes = pathes.filter(m_pathMatcher::matches);
		}
		List<Path> pathList = pathes.toList();
		for ( Path path: pathList ) {
			String suffix = path.toAbsolutePath().toString().substring(prefixLen);
			if ( suffix.charAt(0) == '/' ) {
				suffix = suffix.substring(1);
			}
			String destPath = m_dest + "/" + suffix;
			
			try ( FileBlockIterator blocks = new FileBlockIterator(path) ) {
				m_marmot.copyToHdfsFile(destPath, blocks, m_blockSize);
			}
		}
		
		s_logger.info("uploaded: elapsed={}", watch.stopAndGetElpasedTimeString());
	}

	private static final int BLOCK_SIZE = (int)UnitUtils.parseByteSize("512kb");
	private static class FileBlockIterator implements Iterator<byte[]>, Closeable {
		private InputStream m_is;
		private byte[] m_block;
		private int m_nread;
		
		FileBlockIterator(Path path) throws IOException {
			m_is = new BufferedInputStream(Files.newInputStream(path), BLOCK_SIZE);
			
			m_block = new byte[BLOCK_SIZE];
			m_nread = m_is.read(m_block);
			if ( m_nread < 0 ) {
				IOUtils.closeQuietly(m_is);
			}
		}

		@Override
		public void close() throws IOException {
			IOUtils.closeQuietly(m_is);
			m_is = null;
		}
		
		@Override
		public boolean hasNext() {
			return m_nread > 0;
		}

		@Override
		public byte[] next() {
			byte[] block = m_block;
			if ( m_nread != BLOCK_SIZE ) {
				block = Arrays.copyOf(m_block, m_nread);
			}
			
			try {
				m_block = new byte[BLOCK_SIZE];
				m_nread = m_is.read(m_block);
				
				if ( m_nread < 0 ) {
					IOUtils.closeQuietly(m_is);
				}
			}
			catch ( IOException e ) {
				m_nread = -1;
			}
			
			return block;
		}
	}
}
