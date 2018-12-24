package marmot.command;

import java.io.File;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;
import utils.func.Funcs;

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

	@Option(names={"-f", "-force"}, description="force to create a new dataset")
	private boolean m_force;

	private long m_blockSize = -1;
	@Mixin private UsageHelp m_help;
	
	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		UploadFiles upload = new UploadFiles(marmot, new File(m_srcPath), m_destPath)
									.force(m_force);
		Funcs.when(m_glob != null, () -> upload.glob(m_glob));
		Funcs.when(m_blockSize > 0, () -> upload.blockSize(m_blockSize));
		upload.run();
	}

	@Option(names={"-b", "-block_size"}, paramLabel="nbyte", description="block size (eg: '64mb')")
	private void setBlockSize(String blockSizeStr) {
		m_blockSize = UnitUtils.parseByteSize(blockSizeStr);
	}
}
