package marmot.geo.command;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.command.UsageHelp;
import marmot.externio.shp.ExportDataSetAsShapefile;
import marmot.externio.shp.ExportShapefileParameters;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.async.ProgressiveExecution;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportAsShapefileCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private ExportShapefileParameters m_shpParams;
	@Mixin private UsageHelp m_help;
	
	private static class Params {
		@Parameters(paramLabel="dataset-id", index="0", arity="1..1",
					description={"id of the dataset to export"})
		private String m_dsId;
		
		@Option(names={"-o", "-output_dir"}, paramLabel="output-directory", required=true,
				description={"directory path for the output"})
		private String m_output;
		
		@Option(names={"-f"}, description="force to create a new output directory")
		private boolean m_force;
		
		@Option(names={"-report_interval"}, paramLabel="record count",
				description="progress report interval")
		private int m_interval = -1;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		ExportDataSetAsShapefile export = new ExportDataSetAsShapefile(m_params.m_dsId,
														m_params.m_output, m_shpParams);
		export.setForce(m_params.m_force);
		FOption.when(m_params.m_interval > 0, m_params.m_interval)
				.ifPresent(export::setProgressInterval);
		
		ProgressiveExecution<Long, Long> act = export.start(marmot);
		act.get();
	}
}
