package marmot.geo.command;

import java.io.BufferedWriter;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.command.UsageHelp;
import marmot.externio.ExternIoUtils;
import marmot.externio.geojson.ExportAsGeoJson;
import marmot.externio.geojson.GeoJsonParameters;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExportAsGeoJsonCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private Params m_params;
	@Mixin private GeoJsonParameters m_gjsonParams;
	@Mixin private UsageHelp m_help;
	
	private static class Params {
		@Parameters(paramLabel="dataset-id", index="0", arity="1..1",
					description={"id of the dataset to export"})
		private String m_dsId;
		
		@Option(names={"-o", "-output"}, paramLabel="output_file",
				description={"path to the output CSV file"})
		private String m_output;
		
		@Option(names={"-p", "-pretty"}, description={"path to the output CSV file"})
		private boolean m_pretty;
	}

	@Override
	public void accept(MarmotRuntime marmot) throws Exception {
		ExportAsGeoJson export = new ExportAsGeoJson(m_params.m_dsId)
									.printPrinter(m_params.m_pretty);
		m_gjsonParams.geoJsonSrid().ifPresent(export::setGeoJSONSrid);
		
		FOption<String> output = FOption.ofNullable(m_params.m_output);
		BufferedWriter writer = ExternIoUtils.toWriter(output, m_gjsonParams.charset());
		long count = export.run(marmot, writer);
		
		System.out.printf("done: %d records%n", count);
	}
}
