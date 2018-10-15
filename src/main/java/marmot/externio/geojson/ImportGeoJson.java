package marmot.externio.geojson;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.vavr.control.Option;
import marmot.Column;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.externio.ImportIntoDataSet;
import marmot.externio.ImportParameters;
import marmot.externio.ImportPlanSupplier;
import marmot.rset.RecordSets;
import utils.CommandLine;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportGeoJson extends ImportIntoDataSet {
	protected final GeoJsonParameters m_gjsonParams;
	
	protected abstract Option<Plan> loadMetaPlan();
	
	public static ImportGeoJson from(File file, GeoJsonParameters csvParams,
									ImportParameters importParams) {
		return new ImportCsvFileIntoDataSet(file, csvParams, importParams);
	}
	
	public static ImportGeoJson from(BufferedReader reader, GeoJsonParameters csvParams,
									ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, Option.none(), csvParams, importParams);
	}
	
	public static ImportGeoJson from(BufferedReader reader, Plan plan,
									GeoJsonParameters csvParams,
									ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, Option.some(plan), csvParams,
												importParams);
	}

	private ImportGeoJson(GeoJsonParameters csvParams, ImportParameters importParams) {
		super(importParams);
		
		m_gjsonParams = csvParams;
	}

	@Override
	protected Option<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			Option<Plan> importPlan = loadMetaPlan();
			Option<Plan> prePlan = getPrePlan();
			
			if ( importPlan.isEmpty() && prePlan.isEmpty() ) {
				return Option.none();
			}
			if ( importPlan.isEmpty() ) {
				return prePlan;
			}
			if ( prePlan.isEmpty() ) {
				return importPlan;
			}
			
			return Option.some(Plan.concat(prePlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	private Option<Plan> getPrePlan() {
		Option<String> osrcSrid = m_gjsonParams.sourceSrid();
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		if ( osrcSrid.isDefined() ) {
			String srcSrid = osrcSrid.get();
			if ( !srcSrid.equals(info.srid()) ) {
				return Option.some(new PlanBuilder("import_geojson")
										.transformCrs(info.name(), srcSrid,
												info.srid(), info.name())
										.build());
			}
		}
		
		return Option.none();
	}
	
	private static class ImportCsvFileIntoDataSet extends ImportGeoJson {
		private final File m_start;
		
		ImportCsvFileIntoDataSet(File file, GeoJsonParameters csvParams,
								ImportParameters importParams) {
			super(csvParams, importParams);
			
			m_start = file;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			@SuppressWarnings("resource")
			RecordSet rset = new MultiFileGeoJsonRecordSet(m_start, m_gjsonParams.charset());
			String tarGeomCol = m_params.getGeometryColumnInfo().get().name();
			if ( !"geometry".equals(tarGeomCol) ) {
				RecordSchema schema =rset.getRecordSchema().columnFStream()
										.map(col -> col.name().equals("geometry")
													? new Column(tarGeomCol, col.type()) : col)
										.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
										.build();
				rset = RecordSets.from(schema, rset.fstream());
			}
			
			return rset;
		}

		@Override
		protected Option<Plan> loadMetaPlan() {
			return ImportPlanSupplier.from(m_start).get();
		}
	}
	
	private static class ImportCsvStreamIntoDataSet extends ImportGeoJson {
		private final BufferedReader m_reader;
		private final Option<Plan> m_plan;
		
		ImportCsvStreamIntoDataSet(BufferedReader reader, Option<Plan> plan,
									GeoJsonParameters csvParams, ImportParameters importParams) {
			super(csvParams, importParams);
			
			m_reader = reader;
			m_plan = plan;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			try {
				RecordSet rset = MultiFileGeoJsonRecordSet.parseGeoJson(m_reader);
				String tarGeomCol = m_params.getGeometryColumnInfo().get().name();
				if ( !"geometry".equals(tarGeomCol) ) {
					RecordSchema schema =rset.getRecordSchema().columnFStream()
											.map(col -> col.name().equals("geometry")
														? new Column(tarGeomCol, col.type()) : col)
											.foldLeft(RecordSchema.builder(), (b,c) -> b.addColumn(c))
											.build();
					rset = RecordSets.from(schema, rset.fstream());
				}
				
				return rset;
			}
			catch ( IOException e ) {
				throw new RecordSetException("fails to load MultiFileGeoJsonRecordSet: cause=" + e);
			}
		}

		@Override
		protected Option<Plan> loadMetaPlan() {
			return m_plan;
		}
	}

	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	public static final void runCommand(MarmotRuntime marmot, CommandLine cl) throws IOException {
		File file = new File(cl.getArgument("geojson_file"));
		Charset cs = cl.getOptionString("charset")
						.map(Charset::forName)
						.getOrElse(DEFAULT_CHARSET);
		Option<String> srcSrid = cl.getOptionString("src_srid");
		String dsId = cl.getString("dataset");
		String geomCol = cl.getOptionString("geom_col").getOrElse("the_geom");
		String srid = cl.getString("srid");
		long blkSize = cl.getOptionString("block_size")
						.map(UnitUtils::parseByteSize)
						.getOrElse(-1L);
		boolean force = cl.hasOption("f");
		int reportInterval = cl.getOptionInt("report_interval").getOrElse(-1);
		
		StopWatch watch = StopWatch.start();
		
		ImportParameters params = ImportParameters.create()
												.setDatasetId(dsId)
												.setGeometryColumnInfo(geomCol, srid)
												.setBlockSize(blkSize)
												.setReportInterval(reportInterval)
												.setForce(force);
		GeoJsonParameters gjsonParams = GeoJsonParameters.create()
												.charset(cs);
		srcSrid.forEach(gjsonParams::sourceSrid);
		
		ImportIntoDataSet importFile = ImportGeoJson.from(file, gjsonParams, params);
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
