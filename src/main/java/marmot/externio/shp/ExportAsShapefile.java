package marmot.externio.shp;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.geotools.data.shapefile.ShapefileDumper;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.base.Preconditions;

import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import marmot.RecordSet;
import marmot.geo.geotools.GeoToolsUtils;
import marmot.geo.geotools.MarmotFeatureCollection;
import marmot.rset.RecordSets;
import utils.async.AbstractExecution;
import utils.async.CancellableWork;
import utils.async.Executors;
import utils.async.ProgressiveExecution;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ExportAsShapefile {
	private static final long REPORT_INTERVAL = 100_000;
	
	private final File m_outputDir;
	private final String m_sfTypeName;
	private final ShapefileParameters m_params;
	private boolean m_force = false;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.create();
	private long m_interval = REPORT_INTERVAL;
	
	protected ExportAsShapefile(String outputDir, ShapefileParameters params) {
		Objects.requireNonNull(outputDir, "output directory is null");
		Objects.requireNonNull(params, "ShapefileParameters is null");
		
		m_outputDir = new File(outputDir);
		Preconditions.checkArgument(m_outputDir != null, "invalid output: " + outputDir);
		m_sfTypeName = params.typeName()
							.getOrElse(() -> FilenameUtils.getBaseName(outputDir));
		Preconditions.checkArgument(m_sfTypeName != null && m_sfTypeName.length() > 0,
									"invalid output (typeName is not given): " + outputDir);
		m_params = params;
	}
	
	public void setForce(boolean flag) {
		m_force = flag;
	}
	
	public void setProgressInterval(long count) {
		m_interval = count;
	}
	
	protected ProgressiveExecution<Long,Long> start(RecordSet source, String srid)
		throws IOException {
		ExportExecution exec = new ExportExecution(source, srid);
		Executors.start(exec);
		
		return exec;
	}
	
	private class ExportExecution extends AbstractExecution<Long>
									implements ProgressiveExecution<Long,Long>, CancellableWork {
		private final RecordSet m_source;
		private final String m_srid;
		
		private ExportExecution(RecordSet src, String srid) {
			m_source = src;
			m_srid = srid;
		}

		@Override
		public Observable<Long> getProgressObservable() {
			return m_subject;
		}

		@Override
		public Long executeWork() throws Exception {
			try ( RecordSet rset = RecordSets.reportProgress(m_source, m_subject, m_interval) ) {
				if ( m_force ) {
					FileUtils.forceDelete(m_outputDir);
				}
				FileUtils.forceMkdir(m_outputDir);

				ShapefileDumper dumper = new ShapefileDumper(m_outputDir);
				dumper.setCharset(m_params.charset());
				m_params.splitSize().forEach(size -> {
					dumper.setMaxDbfSize(size);
					dumper.setMaxShpSize(size);
				});
				
				SimpleFeatureType sfType = GeoToolsUtils.toSimpleFeatureType(m_sfTypeName, m_srid,
																			rset.getRecordSchema());
				SimpleFeatureCollection coll = new MarmotFeatureCollection(sfType, ()->rset);
				dumper.dump(coll);
				
				if ( m_aopGuard.get(() -> m_aopState == State.RUNNING) ) {
					return rset.count();
				}
				else {
					throw new CancellationException();
				}
			}
		}
		
		@Override
		public boolean cancelWork() {
			m_source.close();
			return true;
		}
	}
}
