package marmot.geo.query;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;

import marmot.DataSet;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import utils.StopWatch;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.async.CancellableWork;
import utils.async.StartableExecution;
import utils.async.op.AsyncExecutions;
import utils.func.Try;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class IndexScan {
	private static final Logger s_logger = LoggerFactory.getLogger(IndexScan.class);

	private static final int CACHE_COST = 1;
	private static final int NETWORK_COST = 2;
	
	private final MarmotRuntime m_marmot;
	private final DataSet m_ds;
	private final String m_dsId;
	private final RangedClusterEstimate m_est;
	private final Envelope m_range;
	private final PreparedGeometry m_pkey;
	private final long m_sampleCount;
	private final int m_maxLocalCacheCost;
	private final DataSetPartitionCache m_cache;
	private volatile boolean m_usePrefetch = false;
	
	public static IndexScan on(DataSet ds, Envelope range, long sampleCount,
						DataSetPartitionCache cache, int maxLocalCacheCost) {
		return new IndexScan(ds, range, sampleCount, cache, maxLocalCacheCost);
	}
	
	private IndexScan(DataSet ds, Envelope range, long sampleCount,
						DataSetPartitionCache cache, int maxLocalCacheCost) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		Utilities.checkNotNullArgument(range, "query ranage");
		
		m_marmot = ds.getMarmotRuntime();
		m_ds = ds;
		m_dsId = m_ds.getId();
		m_range = range;
		m_sampleCount = sampleCount;
		m_cache = cache;
		m_maxLocalCacheCost = maxLocalCacheCost;
		
		Geometry key = GeoClientUtils.toPolygon(m_range);
		m_pkey = PreparedGeometryFactory.prepare(key);

		// 질의 영역과 겹치는 quad-key들과, 추정되는 결과 레코드의 수를 계산한다.
		m_est = RangedClusterEstimate.about(m_ds, m_range);
	}
	
	public IndexScan usePrefetch(boolean flag) {
		m_usePrefetch = flag;
		return this;
	}
	
	public RecordSet run() {
		// 추정된 결과 레코드 수를 통해 샘플링 비율을 계산한다.
		double ratio = (m_sampleCount > 0)
							? (double)m_sampleCount / m_est.getTotalMatchCount() : 1d;
		final double sampleRatio = ratio > 1 ? 1 : ratio;
		
		double fullRatio = (sampleRatio * m_est.getTotalMatchCount()) / m_ds.getRecordCount();
		if ( fullRatio > 0.7 ) {
			s_logger.info("too large for index-scan, use mixed-scan: id={}", m_dsId);
			
			if ( m_ds.hasThumbnail() && m_sampleCount > 0 ) {
				return ThumbnailScan.on(m_ds, m_range, m_sampleCount).run();
			}
			else {
				return FullScan.on(m_ds)
								.setRange(m_range)
								.setSampleRatio(sampleRatio)
								.run();
			}
		}
		
		//
		// use index-scan with local cache from now on...
		//
		
		// quad-key들 중에서 캐슁되지 않은 cluster들의 갯수를 구한다.
		int nclusters = m_est.getMatchingClusterCount();
		List<String> cachedKeys = FStream.from(m_est.getMatchingClusterKeys())
										.filter(qkey -> m_cache.exists(m_dsId, qkey))
										.toList();
		int remoteIoCount = nclusters - cachedKeys.size();
		int cost = (cachedKeys.size()*CACHE_COST) + (remoteIoCount * NETWORK_COST);
		
		String msg = String.format("ds_id=%s, clusters=%d/%d, cost=%d/%d, guess_count=%d, ratio=%.3f",
									m_dsId, cachedKeys.size(), nclusters, cost, m_maxLocalCacheCost,
									m_est.getTotalMatchCount(), sampleRatio);
		if ( cost > m_maxLocalCacheCost ) {
			if ( m_usePrefetch ) {
				StartableExecution<RecordSet> fg = AsyncExecutions.from(() -> runAtServer(sampleRatio, nclusters, msg));
				StartableExecution<?> bg = forkClusterPrefetcher();
				StartableExecution<RecordSet> exec = AsyncExecutions.backgrounded(fg, bg);
				exec.start();
				
				return exec.getUnchecked();
			}
			else {
				return runAtServer(sampleRatio, nclusters, msg);
			}
		}
		else {
			return runOnLocalCache(sampleRatio, msg);
		}
	}
	
	private RecordSet runAtServer(double sampleRatio, int nclusters, String logMsg) {
		if ( m_ds.hasThumbnail() && m_sampleCount > 0 ) {
			boolean insufficient = m_ds.getThumbnailRatio() < sampleRatio;
			if ( !insufficient ) {
				String tailMsg = String.format(", thumbnail=%.3f, sampling=%.3f",
												m_ds.getThumbnailRatio(), sampleRatio);
				s_logger.info("use thumbnail: {}{}", logMsg, tailMsg);
				try {
					return ThumbnailScan.on(m_ds, m_range, m_sampleCount).run();
				}
				catch ( Exception ignored ) { }
			}
		}
		
		s_logger.info("use index-scan: {}", logMsg);
		
		// 샘플 수가 정의되지 않거나, 대상 데이터세트의 레코드 갯수가 샘플 수보다 작은 경우
		// 데이터세트 전체를 반환한다. 성능을 위해 query() 연산 활용함.
		String planName = String.format("index_scan(ratio=%.3f)", sampleRatio);
		if ( sampleRatio >= 1 ) {
			Plan plan = m_marmot.planBuilder(planName)
								.query(m_dsId, m_range)
								.take(m_sampleCount)
								.build();
			return m_marmot.executeLocally(plan);
		}
		else {
			Plan plan = m_marmot.planBuilder(planName)
								.query(m_dsId, m_range)
								.sample(sampleRatio)
								.take(m_sampleCount)
								.build();
			return m_marmot.executeToRecordSet(plan);
		}
	}
	
	private RecordSet runOnLocalCache(double sampleRatio, String logMsg) {
		s_logger.info("use local-cache: {}", logMsg);
		
		Function<String,FStream<Record>> loader
							= Try.lift((String qk) -> readFromCache(qk, sampleRatio))
									.andThen(d -> d.getOrElse(FStream.empty()));
		FStream<Record> recStream = FStream.from(m_est.getMatchingClusterKeys())
											.flatMapParallel(loader, 3);
		
		return RecordSet.from(m_ds.getRecordSchema(), recStream);
	}
	
	private FStream<Record> readFromCache(String quadKey, double sampleRatio) throws IOException {
		String geomColName = m_ds.getGeometryColumn();
		FStream<Record> matcheds = m_cache.get(m_dsId, quadKey)
											.fstream()
											.filter(r -> {
												Geometry geom = r.getGeometry(geomColName);
												return m_pkey.intersects(geom);
											});
		if ( sampleRatio < 1 ) {
			// quadKey에 해당하는 파티션에 샘플링할 레코드 수를 계산하고
			// 이 수만큼의 레코드만 추출하도록 연산을 추가한다.
			int count = m_est.getMatchingRecordCount(quadKey);
			int takeCount = (int)Math.max(1, Math.round(count * sampleRatio));
			matcheds = matcheds.take(takeCount);
//			long total = m_est.getRelevantRecordCount(quadKey);
//			matcheds = new AdaptableSamplingStream<>(matcheds, total, m_sampleRatio);
		}
		
		return matcheds;
	}
	
	private StartableExecution<Void> forkClusterPrefetcher() {
		FStream<StartableExecution<?>> strm
								= FStream.from(m_est.getMatchingClusterKeys())
										.filter(qkey -> !m_cache.exists(m_dsId, qkey))
										.takeTopK(5, (k1,k2) -> k2.length() - k1.length())
//										.sort((k1,k2) -> k2.length() - k1.length())
										.map(Prefetcher::new);
		return AsyncExecutions.sequential(strm);
	}
	
	private class Prefetcher extends AbstractThreadedExecution<Void>
							implements CancellableWork {
		private final String m_quadKey;
		
		Prefetcher(String quadKey) {
			m_quadKey = quadKey;
		}
		
		@Override
		protected Void executeWork() throws Exception {
			StopWatch watch = StopWatch.start();
			try {
				m_cache.put(m_dsId, m_quadKey, m_ds.readSpatialCluster(m_quadKey));
				
				s_logger.debug("prefetched: ds={}, quadkey={}, elapsed={}",
								m_dsId, m_quadKey, watch.stopAndGetElpasedTimeString());
			}
			catch ( Exception e ) {
				m_cache.remove(m_dsId, m_quadKey);
				s_logger.warn("fails to prefetch: ds=" + m_dsId + ", quadkey=" + m_quadKey
								+ ", cause=" + e);
			}
			return null;
		}

		@Override
		public boolean cancelWork() {
			CompletableFuture.runAsync(() -> Try.run(() -> waitForDone()));
			
			return true;
		}
		
		@Override
		public String toString() {
			return String.format("PartitionPrefetcher[%s]", m_quadKey);
		}
	}
}