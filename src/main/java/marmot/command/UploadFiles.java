package marmot.command;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

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
	
	private void uploadFile(File file) throws FileNotFoundException, IOException {
		if ( m_pathMatcher != null && !m_pathMatcher.matches(file.toPath()) ) {
			return;
		}
		
		StopWatch watch = StopWatch.start();
		
		String dest = m_dest + "/" + file.getName();
		try ( InputStream is = new BufferedInputStream(new FileInputStream(file), BLOCK_SIZE) ) {
			long nbytes = m_marmot.copyToHdfsFile(dest, is, m_blockSize, m_codecName);
			
			watch.stop();
			String velo = UnitUtils.toByteSizeString(Math.round(nbytes / watch.getElapsedInFloatingSeconds()));
			s_logger.info("uploaded: src={}, tar={}, nbytes={}, elapsed={}, velo={}/s",
					file, dest, UnitUtils.toByteSizeString(nbytes), watch.getElapsedSecondString(), velo);
		}
	}
	
	private void uploadDir(File start) throws IOException {
		FStream<Path> pathes = FileUtils.walk(start.toPath()).drop(1).sort();
		if ( m_pathMatcher != null ) {
			pathes = pathes.filter(m_pathMatcher::matches);
		}
		
		String prefix = start.toPath().toAbsolutePath().toString();
		int prefixLen = prefix.length();
		
		for ( Path path: pathes ) {
			StopWatch watch = StopWatch.start();
			
			String suffix = path.toAbsolutePath().toString().substring(prefixLen);
			if ( suffix.charAt(0) == '/' ) {
				suffix = suffix.substring(1);
			}
			String destPath = m_dest + "/" + suffix;
			
			try ( InputStream is = new BufferedInputStream(Files.newInputStream(path), BLOCK_SIZE) ) {
				long nbytes = m_marmot.copyToHdfsFile(destPath, is, m_blockSize, m_codecName);
				
				watch.stop();
				String velo = UnitUtils.toByteSizeString(Math.round(nbytes / watch.getElapsedInFloatingSeconds()));
				s_logger.info("uploaded: src={}, tar={}, nbytes={}, elapsed={}, velo={}/s",
						path, destPath, UnitUtils.toByteSizeString(nbytes), watch.getElapsedSecondString(), velo);
			}
		}
	}
	
	public void run() throws Exception {
		StopWatch watch = StopWatch.start();
		
		if ( m_start.isFile() ) {
			uploadFile(m_start);
		}
		else {
			uploadDir(m_start);
		}
		
		s_logger.info("uploaded: {}, elapsed={}", m_start, watch.getElapsedSecondString());
	}
}
