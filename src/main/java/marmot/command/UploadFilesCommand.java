package marmot.command;

import java.io.File;

import utils.UnitUtils;
import utils.UsageHelp;
import utils.func.CheckedConsumer;
import utils.func.Funcs;

import marmot.MarmotRuntime;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UploadFilesCommand implements CheckedConsumer<MarmotRuntime> {
	@Parameters(paramLabel="src-path", index="0", arity="1..1",
				description="path to the source local file (or directory)")
	private String m_srcPath;
	
	@Parameters(paramLabel="dest-path", index="1", arity="1..1",
				description="HDFS path to the destination directory")
	private String m_destPath;
	
	@Option(names= {"-g", "-glob"}, paramLabel="glob", description="path matcher")
	private String m_glob = null;

	private long m_blockSize = -1;
	
	@Option(names={"-c", "-compression"}, paramLabel="codec_name", description="compression codec name")
	private String m_codecName = null;
	
	@Mixin private UsageHelp m_help;
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		UploadFiles upload = new UploadFiles(marmot, new File(m_srcPath), m_destPath);
		Funcs.runIfNonNull(m_glob, () -> upload.glob(m_glob));
		Funcs.runIf(() -> upload.blockSize(m_blockSize), m_blockSize > 0);
		Funcs.acceptIfNonNull(m_codecName, upload::compressionCodecName);
		upload.run();
	}

	@Option(names={"-b", "-block_size"}, paramLabel="nbyte", description="block size (eg: '64mb')")
	private void setBlockSize(String blockSizeStr) {
		m_blockSize = UnitUtils.parseByteSize(blockSizeStr);
	}
}
