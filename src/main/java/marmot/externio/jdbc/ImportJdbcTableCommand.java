package marmot.externio.jdbc;

import io.vavr.CheckedConsumer;
import marmot.MarmotRuntime;
import marmot.command.ImportParameters;
import marmot.command.UsageHelp;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import utils.StopWatch;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportJdbcTableCommand implements CheckedConsumer<MarmotRuntime> {
	@Mixin private JdbcParameters m_jdbcParams;
	@Mixin private ImportParameters m_importParams;
	@Mixin private UsageHelp m_help;

	@Parameters(paramLabel="table_name", index="0", arity="1..1",
				description={"JDBC table name"})
	private String m_tableName;
	
	@Parameters(paramLabel="dataset_id", index="1", arity="1..1",
			description={"dataset id to import onto"})
	public void setDataSetId(String id) {
		Utilities.checkNotNullArgument(id, "dataset id is null");
		m_importParams.setDataSetId(id);
	}

	@Override
	public void accept(MarmotRuntime marmot) {
		StopWatch watch = StopWatch.start();
		
		ImportJdbcTable importFile = ImportJdbcTable.from(m_tableName, m_jdbcParams,
															m_importParams);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(marmot);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							m_importParams.getDataSetId(), count, watch.getElapsedMillisString(), velo);
	}
}
