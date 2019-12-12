package marmot.externio.shp;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.PlanBuilder;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.command.ImportParameters;
import marmot.dataset.GeometryColumnInfo;
import marmot.externio.ImportIntoDataSet;
import marmot.rset.SingleThreadSuppliedRecordSet;
import marmot.support.MetaPlanLoader;
import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ImportShapefile extends ImportIntoDataSet {
	private final File m_start;
	private final ShapefileParameters m_shpParams;

	public static ImportShapefile from(File start, ShapefileParameters shpParams,
										ImportParameters params) {
		return new ImportShapefile(start, shpParams, params);
	}

	private ImportShapefile(File start, ShapefileParameters shpParams, ImportParameters params) {
		super(params);
		Utilities.checkArgument(params.getGeometryColumnInfo().isPresent(),
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
		FOption<String> oshpSrid = m_shpParams.srid();
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
}
