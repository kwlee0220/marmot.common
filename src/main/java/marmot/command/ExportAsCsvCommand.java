package marmot.command;

import java.io.BufferedWriter;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.externio.ExternIoUtils;
import marmot.externio.csv.CsvParameters;
import marmot.externio.csv.ExportAsCsv;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportAsCsvCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private CsvParameters m_csvParams;
	@Mixin private UsageHelp m_help;
	
	private static class Params {
		@Parameters(paramLabel="dataset-id", index="0", arity="1..1",
					description={"id of the dataset to export"})
		private String m_dsId;
		
		@Option(names={"-o", "-output"}, paramLabel="output_file",
				description={"path to the output CSV file"})
		private String m_output;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		ExportAsCsv export = new ExportAsCsv(m_params.m_dsId, m_csvParams);
		
		FOption<String> output = FOption.ofNullable(m_params.m_output);
		BufferedWriter writer = ExternIoUtils.toWriter(output, m_csvParams.charset());
		export.run(marmot, writer);
	}
}
