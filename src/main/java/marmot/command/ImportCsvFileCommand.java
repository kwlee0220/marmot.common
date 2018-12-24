package marmot.command;

import java.io.File;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.csv.CsvParameters;
import marmot.externio.csv.ImportCsv;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportCsvFileCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private CsvParameters m_csvParams;
	@Mixin private ImportParameters m_importParams;
	@Mixin private UsageHelp m_help;
	
	@Parameters(paramLabel="file_path", index="0", arity="1..1",
				description={"path to the target csv file"})
	private String m_start;

	@Override
	public void accept(MarmotRuntime marmot) {
		StopWatch watch = StopWatch.start();
		
		File csvFilePath = new File(m_start);
		ImportIntoDataSet importFile = ImportCsv.from(csvFilePath, m_csvParams, m_importParams);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(marmot);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
						m_importParams.getDataSetId(), count, watch.getElapsedMillisString(), velo);
	}
}
