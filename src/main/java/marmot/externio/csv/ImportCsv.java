package marmot.externio.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.externio.ImportIntoDataSet;
import marmot.support.MetaPlanLoader;
import utils.Throwables;
import utils.func.FOption;
import utils.func.Tuple;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportCsv extends ImportIntoDataSet {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	protected final CsvParameters m_csvParams;
	
	protected abstract FOption<Plan> loadMetaPlan();
	
	public static ImportCsv from(File file, CsvParameters csvParams, ImportParameters params) {
		return new ImportCsvFileIntoDataSet(file, csvParams, params, "**/*.csv");
	}
	
	public static ImportCsv from(File file, CsvParameters csvParams, ImportParameters params,
								String glob) {
		return new ImportCsvFileIntoDataSet(file, csvParams, params, glob);
	}
	
	public static ImportCsv from(BufferedReader reader, CsvParameters csvParams,
									ImportParameters params) {
		return new ImportCsvStreamIntoDataSet(reader, FOption.empty(), csvParams, params);
	}
	
	public static ImportCsv from(BufferedReader reader, Plan plan, CsvParameters csvParams,
									ImportParameters params) {
		return new ImportCsvStreamIntoDataSet(reader, FOption.of(plan), csvParams, params);
	}

	private ImportCsv(CsvParameters csvParams, ImportParameters params) {
		super(params);
		
		m_csvParams = csvParams.duplicate();
		m_csvParams.charset().ifAbsent(() -> m_csvParams.charset(DEFAULT_CHARSET));
	}

	@Override
	protected FOption<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			FOption<Plan> importPlan = loadMetaPlan();
			FOption<Plan> toPointPlan = getToPointPlan();
			
			if ( importPlan.isAbsent() && toPointPlan.isAbsent() ) {
				return FOption.empty();
			}
			if ( importPlan.isAbsent() ) {
				return toPointPlan;
			}
			if ( toPointPlan.isAbsent() ) {
				return importPlan;
			}
			
			return FOption.of(Plan.concat(toPointPlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}

	private FOption<Plan> getToPointPlan() {
		if ( !m_csvParams.pointColumns().isPresent()
			|| !m_params.getGeometryColumnInfo().isPresent() ) {
			return FOption.empty();
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
		
		return FOption.of(builder.build());
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
		protected FOption<Plan> loadMetaPlan() {
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
		private final FOption<Plan> m_plan;
		
		ImportCsvStreamIntoDataSet(BufferedReader reader, FOption<Plan> plan,
									CsvParameters csvParams, ImportParameters params) {
			this("unknown", reader, plan, csvParams, params);
		}
		
		ImportCsvStreamIntoDataSet(String key, BufferedReader reader, FOption<Plan> plan,
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
		protected FOption<Plan> loadMetaPlan() {
			return m_plan;
		}
	}
}
