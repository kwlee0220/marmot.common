package marmot.externio.shp;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import utils.Preconditions;
import utils.Throwables;
import utils.func.FOption;

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
		Preconditions.checkArgument(params.getGeometryColumnInfo().isPresent(),
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
	protected Optional<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			Optional<Plan> importPlan = MetaPlanLoader.load(m_start);
			Optional<Plan> prePlan = getPrePlan();
			
			if ( importPlan.isEmpty() && prePlan.isEmpty() ) {
				return Optional.empty();
			}
			if ( importPlan.isEmpty() ) {
				return prePlan;
			}
			if ( prePlan.isEmpty() ) {
				return importPlan;
			}
			
			return Optional.of(Plan.concat(prePlan.get(), importPlan.get()));
		}
		catch ( Exception e ) {
			throw Throwables.toRuntimeException(e);
		}
	}
	
	private Optional<Plan> getPrePlan() {
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		FOption<String> oshpSrid = m_shpParams.srid();
		if ( oshpSrid.isPresent() ) {
			String shpSrid = oshpSrid.get();
			if ( !shpSrid.equals(info.srid()) ) {
				return Optional.of(new PlanBuilder("import_shapefile")
										.transformCrs(info.name(), shpSrid, info.srid())
										.build());
			}
		}
		
		return Optional.empty();
	}
}
