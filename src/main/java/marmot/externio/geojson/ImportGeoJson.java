package marmot.externio.geojson;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import marmot.Column;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.externio.ImportIntoDataSet;
import marmot.support.MetaPlanLoader;
import utils.CommandLine;
import utils.StopWatch;
import utils.Throwables;
import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ImportGeoJson extends ImportIntoDataSet {
	protected final GeoJsonParameters m_gjsonParams;
	
	protected abstract FOption<Plan> loadMetaPlan();
	
	public static ImportGeoJson from(File file, GeoJsonParameters csvParams,
									ImportParameters importParams) {
		return new ImportCsvFileIntoDataSet(file, csvParams, importParams);
	}
	
	public static ImportGeoJson from(BufferedReader reader, GeoJsonParameters csvParams,
									ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, FOption.empty(), csvParams, importParams);
	}
	
	public static ImportGeoJson from(BufferedReader reader, Plan plan,
									GeoJsonParameters csvParams,
									ImportParameters importParams) {
		return new ImportCsvStreamIntoDataSet(reader, FOption.of(plan), csvParams,
												importParams);
	}

	private ImportGeoJson(GeoJsonParameters csvParams, ImportParameters importParams) {
		super(importParams);
		
		m_gjsonParams = csvParams;
	}

	@Override
	protected FOption<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			FOption<Plan> importPlan = loadMetaPlan();
			FOption<Plan> prePlan = getPrePlan();
			
			if ( importPlan.isAbsent() && prePlan.isAbsent() ) {
				return FOption.empty();
			}
			if ( importPlan.isAbsent() ) {
				return prePlan;
			}
			if ( prePlan.isAbsent() ) {
				return importPlan;
			}
			
			return FOption.of(Plan.concat(prePlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	private FOption<Plan> getPrePlan() {
		FOption<String> osrcSrid = m_gjsonParams.geoJsonSrid();
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		if ( osrcSrid.isPresent() ) {
			String srcSrid = osrcSrid.get();
			if ( !srcSrid.equals(info.srid()) ) {
				return FOption.of(new PlanBuilder("import_geojson")
										.transformCrs(info.name(), srcSrid, info.srid())
										.build());
			}
		}
		
		return FOption.empty();
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
				rset = RecordSet.from(schema, rset.fstream());
			}
			
			return rset;
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
	
	private static class ImportCsvStreamIntoDataSet extends ImportGeoJson {
		private final BufferedReader m_reader;
		private final FOption<Plan> m_plan;
		
		ImportCsvStreamIntoDataSet(BufferedReader reader, FOption<Plan> plan,
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
					rset = RecordSet.from(schema, rset.fstream());
				}
				
				return rset;
			}
			catch ( IOException e ) {
				throw new RecordSetException("fails to load MultiFileGeoJsonRecordSet: cause=" + e);
			}
		}

		@Override
		protected FOption<Plan> loadMetaPlan() {
			return m_plan;
		}
	}

	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	public static final void runCommand(MarmotRuntime marmot, CommandLine cl) throws IOException {
		File file = new File(cl.getArgument("geojson_file"));
		Charset cs = cl.getOptionString("charset")
						.map(Charset::forName)
						.getOrElse(DEFAULT_CHARSET);
		FOption<String> srcSrid = cl.getOptionString("src_srid");
		String dsId = cl.getString("dataset");
		String geomCol = cl.getOptionString("geom_col").getOrElse("the_geom");
		String srid = cl.getString("srid");
		long blkSize = cl.getOptionString("block_size")
						.map(UnitUtils::parseByteSize)
						.getOrElse(-1L);
		boolean force = cl.hasOption("f");
		int reportInterval = cl.getOptionInt("report_interval").getOrElse(-1);
		
		StopWatch watch = StopWatch.start();
		
		ImportParameters params = new ImportParameters();
		params.setDataSetId(dsId);
		params.setGeometryColumnInfo(geomCol, srid);
		params.setBlockSize(blkSize);
		params.setReportInterval(reportInterval);
		params.setForce(force);
		
		GeoJsonParameters gjsonParams = GeoJsonParameters.create()
												.charset(cs);
		srcSrid.ifPresent(gjsonParams::geoJsonSrid);
		
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
