package marmot.externio.shp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.externio.ImportIntoDataSet;
import marmot.rset.SingleThreadSuppliedRecordSet;
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
public class ImportShapefile extends ImportIntoDataSet {
	private final File m_start;
	private final ShapefileParameters m_shpParams;

	public static ImportShapefile from(File start, ShapefileParameters shpParams,
										ImportParameters importParams) {
		return new ImportShapefile(start, shpParams, importParams);
	}

	private ImportShapefile(File start, ShapefileParameters shpParams,
							ImportParameters importParams) {
		super(importParams);
		Preconditions.checkArgument(importParams.getGeometryColumnInfo().isPresent(),
									"GeometryColumnInfo is missing");
		
		m_start = start;
		m_shpParams = shpParams;
	}

	@Override
	protected RecordSet loadRecordSet(MarmotRuntime marmot) {
		try {
			RecordSchema schema = ShapefileRecordSet.loadRecordSchema(m_start,
																m_shpParams.charset());
			
			Supplier<RecordSet> suppl = () -> new ShapefileRecordSet(m_start,
																m_shpParams.charset());
			return SingleThreadSuppliedRecordSet.start(schema, suppl);
		}
		catch ( IOException e ) {
			throw new RecordSetException("fails to load ShapefileRecordSet: cause=" + e);
		}
	}

	@Override
	protected FOption<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			FOption<Plan> importPlan = MetaPlanLoader.load(m_start);
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
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		FOption<String> oshpSrid = m_shpParams.shpSrid();
		if ( oshpSrid.isPresent() ) {
			String shpSrid = oshpSrid.get();
			if ( !shpSrid.equals(info.srid()) ) {
				return FOption.of(new PlanBuilder("import_shapefile")
										.transformCrs(info.name(), shpSrid, info.srid())
										.build());
			}
		}
		
		return FOption.empty();
	}
	
	public static final void run(MarmotRuntime marmot, CommandLine cl) throws IOException {
		File shpFile = new File(cl.getArgument("shp_file"));
		String dsId = cl.getString("dataset");
		FOption<Charset> charset = cl.getOptionString("charset")
									.map(Charset::forName);
		FOption<String> shpSrid = cl.getOptionString("shp_srid");
		String srid = cl.getString("srid");
		long blkSize = cl.getOptionString("block_size")
						.map(UnitUtils::parseByteSize)
						.getOrElse(-1L);
		int reportInterval = cl.getOptionInt("report_interval").getOrElse(-1);
		boolean force = cl.hasOption("f");
		boolean append = cl.hasOption("a");
		
		StopWatch watch = StopWatch.start();
		
		
		ImportParameters params = new ImportParameters();
		params.setDataSetId(dsId);
		params.setGeometryColumnInfo("the_geom", srid);
		params.setBlockSize(blkSize);
		params.setReportInterval(reportInterval);
		params.setForce(force);
		params.setAppend(append);
		
		ShapefileParameters shpParams = ShapefileParameters.create();
		charset.ifPresent(shpParams::charset);
		shpSrid.ifPresent(shpParams::shpSrid);
		
		ImportShapefile importFile = ImportShapefile.from(shpFile, shpParams, params);
		importFile.getProgressObservable()
					.subscribe(report -> {
						double velo = report / watch.getElapsedInFloatingSeconds();
						System.out.printf("imported: count=%d, elapsed=%s, velo=%.1f/s%n",
										report, watch.getElapsedMillisString(), velo);
					});
		long count = importFile.run(marmot);
		
		double velo = count / watch.getElapsedInFloatingSeconds();
		System.out.printf("imported: dataset=%s count=%d elapsed=%s, velo=%.1f/s%n",
							dsId, count, watch.getElapsedMillisString(), velo);
	}
}
