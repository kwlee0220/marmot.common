package marmot.geo.command;

import marmot.MarmotRuntime;
import marmot.analysis.module.geo.arc.ArcBufferParameters;
import marmot.command.UsageHelp;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.UnitUtils;
import utils.func.CheckedConsumer;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
abstract class ArcBufferCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private UsageHelp m_help;
	
	public static class Params {
		@Parameters(paramLabel="input_dataset", index="0", arity="1..1", description={"input dataset id"})
		private String m_inputDsId;
		
		@Parameters(paramLabel="output_dataset", index="1", arity="1..1", description={"output dataset id"})
		private String m_outputDsId;
		
		double m_dist = -1;
		@Parameters(paramLabel="distance", index="2", arity="1..1", description={"buffer distance"})
		void setDistance(String distExpr) {
			m_dist = UnitUtils.parseLengthInMeter(distExpr);
		}
		
		@Option(names={"-c", "-compress"}, description="compression codec name")
		private FOption<String> m_codecName = FOption.empty();

		private FOption<Long> m_blkSize = FOption.empty();
		@Option(names={"-b", "-block_size"}, paramLabel="nbyte", description="block size (eg: '64mb')")
		public void setBlockSize(String blockSizeStr) {
			m_blkSize = FOption.ofNullable(UnitUtils.parseByteSize(blockSizeStr));
		}

		@Option(names={"-f", "-force"}, description="force to create a new dataset")
		private boolean m_force = false;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		ArcBufferParameters params = new ArcBufferParameters();
		params.setInputDataset(m_params.m_inputDsId);
		params.setOutputDataset(m_params.m_outputDsId);
		params.setDistance(m_params.m_dist);
		params.setDissolve(true);
		
		params.setForce(m_params.m_force);
		m_params.m_codecName.ifPresent(params::setCompressionCodecName);
		m_params.m_blkSize.ifPresent(params::setBlockSize);
		
		marmot.executeProcess("arc_buffer", params.toMap());
	}
}
