package marmot.externio.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.ImportParameters;
import marmot.support.MetaPlanLoader;
import utils.CSV;
import utils.CommandLine;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportCsv extends ImportIntoDataSet {
	protected final CsvParameters m_csvParams;
	
	protected abstract Option<Plan> loadMetaPlan();
	
	public static ImportCsv from(File file, CsvParameters csvParams,
											ImportParameters importParams) {
		return new ImportCsvFileIntoDataSet(file, csvParams, importParams);
	}
	
	public static ImportCsv from(BufferedReader reader, CsvParameters csvParams,
											ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, Option.none(), csvParams, importParams);
	}
	
	public static ImportCsv from(BufferedReader reader, Plan plan,
											CsvParameters csvParams,
											ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, Option.some(plan), csvParams,
												importParams);
	}

	private ImportCsv(CsvParameters csvParams, ImportParameters importParams) {
		super(importParams);
		
		m_csvParams = csvParams;
	}

	@Override
	protected Option<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			Option<Plan> importPlan = loadMetaPlan();
			Option<Plan> toPointPlan = getToPointPlan();
			
			if ( importPlan.isEmpty() && toPointPlan.isEmpty() ) {
				return Option.none();
			}
			if ( importPlan.isEmpty() ) {
				return toPointPlan;
			}
			if ( toPointPlan.isEmpty() ) {
				return importPlan;
			}
			
			return Option.some(Plan.concat(toPointPlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}

	private Option<Plan> getToPointPlan() {
		if ( !m_csvParams.pointColumn().isDefined()
			|| !m_params.getGeometryColumnInfo().isDefined() ) {
			return Option.none();
		}
		
		PlanBuilder builder = new PlanBuilder("import_csv");
		
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		Tuple2<String,String> ptCols = m_csvParams.pointColumn().get();
		builder = builder.toPoint(ptCols._1, ptCols._2, info.name());
		
		String prjExpr = String.format("%s,*-{%s,%s,%s}", info.name(), info.name(),
															ptCols._1, ptCols._2);
		builder = builder.project(prjExpr);
			
		if ( m_csvParams.csvSrid().isDefined() ) {
			String srcSrid = m_csvParams.csvSrid().get();
			if ( !srcSrid.equals(info.srid()) ) {
				builder = builder.transformCrs(info.name(), srcSrid, info.srid());
			}
		}
		
		return Option.some(builder.build());
	}
	
	private static class ImportCsvFileIntoDataSet extends ImportCsv {
		private final File m_start;
		
		ImportCsvFileIntoDataSet(File file, CsvParameters csvParams, ImportParameters importParams) {
			super(csvParams, importParams);
			
			m_start = file;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			return new MultiFileCsvRecordSet(m_start, m_csvParams);
		}

		@Override
		protected Option<Plan> loadMetaPlan() {
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
		private final Option<Plan> m_plan;
		
		ImportCsvStreamIntoDataSet(BufferedReader reader, Option<Plan> plan,
									CsvParameters csvParams, ImportParameters importParams) {
			super(csvParams, importParams);
			
			m_reader = reader;
			m_plan = plan;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			try {
				return CsvRecordSet.from(m_reader, m_csvParams);
			}
			catch ( IOException e ) {
				throw new RecordSetException("fails to load CsvRecordSet: cause=" + e);
			}
		}

		@Override
		protected Option<Plan> loadMetaPlan() {
			return m_plan;
		}
	}

	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	public static final void runCommand(MarmotRuntime marmot, CommandLine cl) throws IOException {
		File file = new File(cl.getArgument("csv_file"));
		Charset charset = cl.getOptionString("charset")
							.map(Charset::forName)
							.getOrElse(DEFAULT_CHARSET);
		Option<Character> delim = cl.getOptionString("delim").map(s -> s.charAt(0));
		
		Option<Character> quote = cl.getOptionString("quote").map(s -> s.charAt(0));
		Option<String[]> header = cl.getOptionString("header")
									.map(s -> CSV.parseAsArray(s, ',', '\\'));
		boolean headerFirst = cl.hasOption("header_first");
		boolean trimField = cl.hasOption("trim_field");
		Option<String> nullString = cl.getOptionString("null_string");
		Option<String> pointCols = cl.getOptionString("point_col");
		Option<String> csvSrid = cl.getOptionString("csv_srid");
		String dsId = cl.getString("dataset");
		String geomCol = cl.getOptionString("geom_col").getOrNull();
		String srid = cl.getOptionString("srid").getOrNull();
		long blkSize = cl.getOptionString("block_size")
						.map(UnitUtils::parseByteSize)
						.getOrElse(-1L);
		boolean force = cl.hasOption("f");
		boolean append = cl.hasOption("a");
		int reportInterval = cl.getOptionInt("report_interval").getOrElse(-1);
		
		if ( cl.hasOption("wgs84") && csvSrid.isEmpty() ) {
			csvSrid = Option.some("EPSG:4326");
		}
		
		StopWatch watch = StopWatch.start();
		
		ImportParameters importParams = ImportParameters.create()
														.setDatasetId(dsId)
														.setBlockSize(blkSize)
														.setReportInterval(reportInterval)
														.setForce(force)
														.setAppend(append);
		if ( geomCol != null && srid != null ) {
			importParams.setGeometryColumnInfo(geomCol, srid);
		}
		
		CsvParameters csvParams = CsvParameters.create()
												.charset(charset)
												.headerFirst(headerFirst)
												.trimField(trimField);
		delim.forEach(csvParams::delimiter);
		quote.forEach(csvParams::quote);
		header.forEach(csvParams::header);
		nullString.forEach(csvParams::nullString);
		pointCols.forEach(csvParams::pointColumn);
		csvSrid.forEach(csvParams::csvSrid);
		
		ImportIntoDataSet importFile = ImportCsv.from(file, csvParams, importParams);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.0f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(marmot);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							dsId, count, watch.getElapsedMillisString(), velo);
	}
}
