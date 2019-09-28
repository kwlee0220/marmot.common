package marmot.command;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotRuntime;
import utils.StopWatch;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;
import utils.io.FileUtils;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UploadFiles {
	private static final Logger s_logger = LoggerFactory.getLogger(UploadFiles.class);
	private static final int BLOCK_SIZE = (int)UnitUtils.parseByteSize("256kb");
	
	private final MarmotRuntime m_marmot;
	private final File m_start;
	private PathMatcher m_pathMatcher;
	private final String m_dest;
	private FOption<Long> m_blockSize = FOption.empty();
	private FOption<String> m_codecName = FOption.empty();
	
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
	
	public FOption<String> compressionCodecName() {
		return m_codecName;
	}
	
	public UploadFiles compressionCodecName(String codecName) {
		m_codecName = FOption.ofNullable(codecName);
		return this;
	}
	
	public void run() throws Exception {
		StopWatch watch = StopWatch.start();
		
		String prefix = m_start.toPath().toAbsolutePath().toString();
		int prefixLen = prefix.length();
		
		FStream<Path> pathes = FileUtils.walk(m_start.toPath())
										.drop(1)	// root 자신을 제외시킴
										.sort();
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
			
			try ( InputStream is = new BufferedInputStream(Files.newInputStream(path), BLOCK_SIZE) ) {
				m_marmot.copyToHdfsFile(destPath, is, m_blockSize, m_codecName);
			}
		}
		
		s_logger.info("uploaded: elapsed={}", watch.stopAndGetElpasedTimeString());
	}
}
