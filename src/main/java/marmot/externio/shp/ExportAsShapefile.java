package marmot.externio.shp;

import java.io.File;
import java.io.IOException;
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
import marmot.RecordSets.CountingRecordSet;
import marmot.geo.geotools.MarmotFeatureCollection;
import marmot.geo.geotools.SimpleFeatures;
import utils.StopWatch;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.async.CancellableWork;
import utils.async.ProgressiveExecution;
import utils.func.FOption;
import utils.func.Try;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ExportAsShapefile {
	private final File m_outputDir;
	private final String m_sfTypeName;
	private final ShapefileParameters m_params;
	private boolean m_force = false;
	private final BehaviorSubject<Long> m_subject = BehaviorSubject.create();
	private FOption<Long> m_interval = FOption.empty();
	
	protected ExportAsShapefile(String outputDir, ShapefileParameters params) {
		Utilities.checkNotNullArgument(outputDir, "output directory is null");
		Utilities.checkNotNullArgument(params, "ShapefileParameters is null");
		
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
		m_interval = FOption.of(count);
	}
	
	protected ProgressiveExecution<Long,Long> start(RecordSet source, String srid)
		throws IOException {
		ExportExecution exec = new ExportExecution(source, srid);
		exec.start();
		
		return exec;
	}
	
	private class ExportExecution extends AbstractThreadedExecution<Long>
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
			StopWatch watch = StopWatch.start();
			
			RecordSet src;
 			if ( m_interval.isPresent() ) {
				src = m_source.reportProgress(m_subject, m_interval.get());
				m_subject.subscribe(count -> System.out.printf("count=%,d, elapsed=%s%n",
															count, watch.getElapsedSecondString()));
			}
			else {
				src = m_source;
			}
			
			try ( CountingRecordSet rset = src.asCountingRecordSet() ) {
				if ( m_force ) {
					Try.run(() -> FileUtils.forceDelete(m_outputDir));
				}
				FileUtils.forceMkdir(m_outputDir);

				ShapefileDumper dumper = new ShapefileDumper(m_outputDir);
				dumper.setCharset(m_params.charset());
				m_params.splitSize().ifPresent(size -> {
					dumper.setMaxDbfSize(size);
					dumper.setMaxShpSize(size);
				});
				
				SimpleFeatureType sfType = SimpleFeatures.toSimpleFeatureType(m_sfTypeName, m_srid,
																			rset.getRecordSchema());
				SimpleFeatureCollection coll = new MarmotFeatureCollection(sfType, ()->rset);
				dumper.dump(coll);
				
				if ( isRunning() ) {
					return rset.getCount();
				}
				else if ( isCancelRequested() ) {
					throw new CancellationException();
				}
				
				throw new IllegalStateException("unexpected state: " + getState());
			}
		}
		
		@Override
		public boolean cancelWork() {
			m_source.close();
			return true;
		}
	}
}
