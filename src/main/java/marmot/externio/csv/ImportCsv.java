package marmot.externio.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.vavr.Tuple2;
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

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportCsv extends ImportIntoDataSet {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	protected final CsvParameters m_csvOptions;
	
	protected abstract FOption<Plan> loadMetaPlan();
	
	public static ImportCsv from(File file, CsvParameters csvOpts, ImportParameters importParams) {
		return new ImportCsvFileIntoDataSet(file, csvOpts, importParams);
	}
	
	public static ImportCsv from(BufferedReader reader, CsvParameters csvOpts,
								ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, FOption.empty(), csvOpts, importParams);
	}
	
	public static ImportCsv from(BufferedReader reader, Plan plan, CsvParameters csvOpts,
								ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, FOption.of(plan), csvOpts,
												importParams);
	}

	private ImportCsv(CsvParameters csvOpts, ImportParameters importParams) {
		super(importParams);
		
		m_csvOptions = csvOpts.duplicate();
		m_csvOptions.charset().ifAbsent(() -> m_csvOptions.charset(DEFAULT_CHARSET));
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
		if ( !m_csvOptions.pointColumns().isPresent()
			|| !m_params.getGeometryColumnInfo().isPresent() ) {
			return FOption.empty();
		}
		
		PlanBuilder builder = new PlanBuilder("import_csv");
		
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		Tuple2<String,String> ptCols = m_csvOptions.pointColumns().get();
		builder = builder.toPoint(ptCols._1, ptCols._2, info.name());
		
		String prjExpr = String.format("%s,*-{%s,%s,%s}", info.name(), info.name(),
															ptCols._1, ptCols._2);
		builder = builder.project(prjExpr);
			
		if ( m_csvOptions.srid().isPresent() ) {
			String srcSrid = m_csvOptions.srid().get();
			if ( !srcSrid.equals(info.srid()) ) {
				builder = builder.transformCrs(info.name(), srcSrid, info.srid());
			}
		}
		
		return FOption.of(builder.build());
	}
	
	private static class ImportCsvFileIntoDataSet extends ImportCsv {
		private final File m_start;
		
		ImportCsvFileIntoDataSet(File file, CsvParameters csvOpts, ImportParameters importParams) {
			super(csvOpts, importParams);
			
			m_start = file;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			return new MultiFileCsvRecordSet(m_start, m_csvOptions);
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
		private final BufferedReader m_reader;
		private final FOption<Plan> m_plan;
		
		ImportCsvStreamIntoDataSet(BufferedReader reader, FOption<Plan> plan,
									CsvParameters csvOpts, ImportParameters importParams) {
			super(csvOpts, importParams);
			
			m_reader = reader;
			m_plan = plan;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			try {
				return CsvRecordSet.from(m_reader, m_csvOptions);
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
