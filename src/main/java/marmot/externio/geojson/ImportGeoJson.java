package marmot.externio.geojson;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

import utils.Throwables;
import utils.func.FOption;

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
public abstract class ImportGeoJson extends ImportIntoDataSet {
	protected final GeoJsonParameters m_gjsonParams;
	
	protected abstract Optional<Plan> loadMetaPlan();
	
	public static ImportGeoJson from(File file, GeoJsonParameters geojsonParams,
									ImportParameters params) {
		return new ImportGeoJsonFileIntoDataSet(file, geojsonParams, params);
	}
	
	public static ImportGeoJson from(BufferedReader reader, GeoJsonParameters geojParams,
									ImportParameters params) {
		return new ImportGeoJsonStreamIntoDataSet(reader, Optional.empty(), geojParams, params);
	}
	
	public static ImportGeoJson from(BufferedReader reader, Plan plan,
									GeoJsonParameters geojParams, ImportParameters params) {
		return new ImportGeoJsonStreamIntoDataSet(reader, Optional.of(plan), geojParams,
												params);
	}

	private ImportGeoJson(GeoJsonParameters geojParams, ImportParameters params) {
		super(params);
		
		m_gjsonParams = geojParams;
	}

	@Override
	protected Optional<Plan> loadImportPlan(MarmotRuntime marmot) {
		try {
			Optional<Plan> importPlan = loadMetaPlan();
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
		FOption<String> osrcSrid = m_gjsonParams.geoJsonSrid();
		GeometryColumnInfo info = m_params.getGeometryColumnInfo().get();
		if ( osrcSrid.isPresent() ) {
			String srcSrid = osrcSrid.get();
			if ( !srcSrid.equals(info.srid()) ) {
				return Optional.of(new PlanBuilder("import_geojson")
										.transformCrs(info.name(), srcSrid, info.srid())
										.build());
			}
		}
		
		return Optional.empty();
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
		protected Optional<Plan> loadMetaPlan() {
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
		private final Optional<Plan> m_plan;
		
		ImportGeoJsonStreamIntoDataSet(BufferedReader reader, Optional<Plan> plan,
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
		protected Optional<Plan> loadMetaPlan() {
			return m_plan;
		}
	}
}
