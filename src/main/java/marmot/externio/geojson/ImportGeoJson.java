package marmot.externio.geojson;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

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
public abstract class ImportGeoJson extends ImportIntoDataSet {
	protected final GeoJsonParameters m_gjsonParams;
	
	protected abstract FOption<Plan> loadMetaPlan();
	
	public static ImportGeoJson from(File file, GeoJsonParameters geojsonParams,
									ImportParameters params) {
		return new ImportGeoJsonFileIntoDataSet(file, geojsonParams, params);
	}
	
	public static ImportGeoJson from(BufferedReader reader, GeoJsonParameters geojParams,
									ImportParameters params) {
		return new ImportGeoJsonStreamIntoDataSet(reader, FOption.empty(), geojParams, params);
	}
	
	public static ImportGeoJson from(BufferedReader reader, Plan plan,
									GeoJsonParameters geojParams, ImportParameters params) {
		return new ImportGeoJsonStreamIntoDataSet(reader, FOption.of(plan), geojParams,
												params);
	}

	private ImportGeoJson(GeoJsonParameters geojParams, ImportParameters params) {
		super(params);
		
		m_gjsonParams = geojParams;
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
	
	private static class ImportGeoJsonFileIntoDataSet extends ImportGeoJson {
		private final File m_start;
		
		ImportGeoJsonFileIntoDataSet(File file, GeoJsonParameters csvParams,
									ImportParameters params) {
			super(csvParams, params);
			
			m_start = file;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			String tarGeomCol = m_params.getGeometryColumnInfo().get().name();
			return new MultiFileGeoJsonRecordSet(m_start, tarGeomCol, m_gjsonParams.charset());
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
	
	private static class ImportGeoJsonStreamIntoDataSet extends ImportGeoJson {
		private final BufferedReader m_reader;
		private final FOption<Plan> m_plan;
		
		ImportGeoJsonStreamIntoDataSet(BufferedReader reader, FOption<Plan> plan,
									GeoJsonParameters geojParams, ImportParameters params) {
			super(geojParams, params);
			
			m_reader = reader;
			m_plan = plan;
		}

		@Override
		protected RecordSet loadRecordSet(MarmotRuntime marmot) {
			try {
				String tarGeomCol = m_params.getGeometryColumnInfo().get().name();
				return MultiFileGeoJsonRecordSet.parseGeoJson(m_reader, tarGeomCol);
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
}
