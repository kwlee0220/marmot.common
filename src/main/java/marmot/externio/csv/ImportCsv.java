package marmot.externio.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import utils.Throwables;
import utils.Tuple;
import utils.func.Optionals;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.dataset.GeometryColumnInfo;
import marmot.externio.ImportIntoDataSet;
import marmot.support.MetaPlanLoader;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportCsv extends ImportIntoDataSet {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	protected final CsvParameters m_csvParams;
	
	protected abstract Optional<Plan> loadMetaPlan();
	
	public static ImportCsv from(File file, CsvParameters csvParams, ImportParameters params) {
		return new ImportCsvFileIntoDataSet(file, csvParams, params, "**/*.csv");
	}
	
	public static ImportCsv from(File file, CsvParameters csvParams, ImportParameters params,
								String glob) {
		return new ImportCsvFileIntoDataSet(file, csvParams, params, glob);
	}
	
	public static ImportCsv from(BufferedReader reader, CsvParameters csvParams,
									ImportParameters params) {
		return new ImportCsvStreamIntoDataSet(reader, Optional.empty(), csvParams, params);
	}
	
	public static ImportCsv from(BufferedReader reader, Plan plan, CsvParameters csvParams,
									ImportParameters params) {
		return new ImportCsvStreamIntoDataSet(reader, Optional.of(plan), csvParams, params);
	}

	private ImportCsv(CsvParameters csvParams, ImportParameters params) {
		super(params);
		
		m_csvParams = csvParams.duplicate();
		Optionals.ifAbsent(m_csvParams.charset(), () -> m_csvParams.charset(DEFAULT_CHARSET));
	}

	@Override
	protected Optional<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			Optional<Plan> importPlan = loadMetaPlan();
			Optional<Plan> toPointPlan = getToPointPlan();
			
			if ( importPlan.isEmpty() && toPointPlan.isEmpty() ) {
				return Optional.empty();
			}
			if ( importPlan.isEmpty() ) {
				return toPointPlan;
			}
			if ( toPointPlan.isEmpty() ) {
				return importPlan;
			}
			
			return Optional.of(Plan.concat(toPointPlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}

	private Optional<Plan> getToPointPlan() {
		if ( !m_csvParams.pointColumns().isPresent()
			|| !m_params.getGeometryColumnInfo().isPresent() ) {
			return Optional.empty();
		}
		
		PlanBuilder builder = new PlanBuilder("import_csv");
		
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		Tuple<String,String> ptCols = m_csvParams.pointColumns().get();
		builder = builder.toPoint(ptCols._1, ptCols._2, info.name());
		
		String prjExpr = String.format("%s,*-{%s,%s,%s}", info.name(), info.name(),
															ptCols._1, ptCols._2);
		builder = builder.project(prjExpr);
			
		if ( m_csvParams.srid().isPresent() ) {
			String srcSrid = m_csvParams.srid().get();
			if ( !srcSrid.equals(info.srid()) ) {
				builder = builder.transformCrs(info.name(), srcSrid, info.srid());
			}
		}
		
		return Optional.of(builder.build());
	}
	
	private static class ImportCsvFileIntoDataSet extends ImportCsv {
		private final File m_start;
		private final String m_glob;
		
		ImportCsvFileIntoDataSet(File file, CsvParameters csvParams,
									ImportParameters params, String glob) {
			super(csvParams, params);
			
			m_start = file;
			m_glob = glob;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			return new MultiFileCsvRecordSet(m_start, m_csvParams, m_glob);
		}

		@Override
		protected Optional<Plan> loadMetaPlan() {
			try {
				return MetaPlanLoader.load(m_start);
			}
			catch ( IOException e ) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private static class ImportCsvStreamIntoDataSet extends ImportCsv {
		private final String m_key;
		private final BufferedReader m_reader;
		private final Optional<Plan> m_plan;
		
		ImportCsvStreamIntoDataSet(BufferedReader reader, Optional<Plan> plan,
									CsvParameters csvParams, ImportParameters params) {
			this("unknown", reader, plan, csvParams, params);
		}
		
		ImportCsvStreamIntoDataSet(String key, BufferedReader reader, Optional<Plan> plan,
									CsvParameters csvParams, ImportParameters params) {
			super(csvParams, params);
			
			m_key = key;
			m_reader = reader;
			m_plan = plan;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			try {
				return CsvRecordSet.from(m_key, m_reader, m_csvParams);
			}
			catch ( IOException e ) {
				throw new RecordSetException("fails to load CsvRecordSet: cause=" + e);
			}
		}

		@Override
		protected Optional<Plan> loadMetaPlan() {
			return m_plan;
		}
	}
}
