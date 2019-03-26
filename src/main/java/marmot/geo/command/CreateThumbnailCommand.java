package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.command.UsageHelp;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateThumbnailCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private UsageHelp m_help;
	
	public static class Params {
		@Parameters(paramLabel="dataset", index="0", arity="1..1",
					description={"dataset id for thumbnail"})
		private String m_dsId;

		@Parameters(paramLabel="sample_count", index="1", arity="1..1",
					description={"sample count"})
		private long m_sampleCount;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		StopWatch watch = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(m_params.m_dsId);
		ds.createThumbnail((int)m_params.m_sampleCount);
		
		System.out.printf("nsmaples=%,d, elapsed time: %s%n",
						m_params.m_sampleCount, watch.stopAndGetElpasedTimeString());
	}
}
