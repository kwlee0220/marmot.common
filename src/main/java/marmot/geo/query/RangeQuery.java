package marmot.geo.query;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Record;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.geo.GeoClientUtils;
import marmot.geo.query.RangeQueryEstimate.ClusterEstimate;
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
public class RangeQuery {
	private static final Logger s_logger = LoggerFactory.getLogger(RangeQuery.class);

	private static final float CACHE_COST = 1f;
	private static final float NETWORK_COST = 2.5f;
	
	private final DataSet m_ds;
	private final String m_dsId;
	private final Envelope m_range;
//	private final RangeQueryEstimate m_est;
	private final PreparedGeometry m_pkey;
	private final int m_sampleCount;
	private final int m_maxLocalCacheCost;
	private final PartitionCache m_cache;
	private volatile boolean m_usePrefetch = false;
	
	RangeQuery(DataSet ds, Envelope range, int sampleCount, PartitionCache cache,
				boolean usePrefetch, int maxLocalCacheCost) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		Utilities.checkNotNullArgument(range, "query ranage");
		Utilities.checkNotNullArgument(cache, "DataSetPartitionCache");
		Utilities.checkArgument(maxLocalCacheCost > 0, "MaxLocalCacheCost > 0, but " + maxLocalCacheCost);
		
		m_ds = ds;
		m_dsId = m_ds.getId();
		m_range = range;
		m_sampleCount = sampleCount;
		m_cache = cache;
		m_usePrefetch = usePrefetch;
		m_maxLocalCacheCost = maxLocalCacheCost;
		
		Geometry key = GeoClientUtils.toPolygon(m_range);
		m_pkey = PreparedGeometryFactory.prepare(key);
	}
	
	public RangeQuery usePrefetch(boolean flag) {
		m_usePrefetch = flag;
		return this;
	}
	
	public RecordSet run() throws Exception {
		if ( !m_ds.hasSpatialIndex() ) {
			if ( m_ds.getRecordCount() <= m_sampleCount ) {
				return m_ds.read();
			}
		}
		
		// 질의 영역과 겹치는 quad-key들과, 추정되는 결과 레코드의 수를 계산한다.
		RangeQueryEstimate est = m_ds.estimateRangeQuery(m_range);
		
		// 추정된 결과 레코드 수를 통해 샘플링 비율을 계산한다.
		double ratio = (m_sampleCount > 0)
						? (double)m_sampleCount / est.getMatchCount() : 1d;
		final double sampleRatio = Math.min(ratio, 1);
		
		// quad-key들 중에서 캐슁되지 않은 cluster들의 갯수를 구한다.
		List<ClusterEstimate> clusters = est.getClusterEstimates();
		int nclusters = clusters.size();
		List<String> cachedKeys = FStream.from(clusters)
										.map(ClusterEstimate::getQuadKey)
										.filter(key -> m_cache.exists(m_dsId, key))
										.toList();
		int remoteIoCount = nclusters - cachedKeys.size();
		int cost = Math.round((cachedKeys.size()*CACHE_COST) + (remoteIoCount * NETWORK_COST));
		
		String msg = String.format("ds_id=%s, clusters=%d/%d, cost=%d/%d, guess_count=%d, ratio=%.3f",
									m_dsId, cachedKeys.size(), nclusters, cost, m_maxLocalCacheCost,
									est.getMatchCount(), sampleRatio);
		if ( cost > m_maxLocalCacheCost ) {
			if ( m_usePrefetch ) {
				StartableExecution<RecordSet> fg = AsyncExecutions.from(() -> queryAtServer(msg));
				StartableExecution<?> bg = forkClusterPrefetcher(est);
				StartableExecution<RecordSet> exec = AsyncExecutions.backgrounded(fg, bg);
				exec.start();
				
				return exec.getUnchecked();
			}
			else {
				return queryAtServer(msg);
			}
		}
		else {
			return runOnLocalCache(sampleRatio, est, msg);
		}
	}
	
	private RecordSet queryAtServer(String logMsg) {
		s_logger.info("query range at server: {}", logMsg);
		return m_ds.queryRange(m_range, m_sampleCount);
	}
	
	private RecordSet runOnLocalCache(double ratio, RangeQueryEstimate est, String logMsg) {
		s_logger.info("use local-cache: {}", logMsg);
		
		Function<String,FStream<Record>> loader
							= Try.lift((String qk) -> readPartitionCache(qk, est, ratio))
									.andThen(d -> d.getOrElse(FStream.empty()));
		FStream<Record> recStream = FStream.from(est.getClusterEstimates())
											.map(ClusterEstimate::getQuadKey)
											.flatMapParallel(loader, 3);
		
		return RecordSet.from(m_ds.getRecordSchema(), recStream);
	}
	
	private FStream<Record> readPartitionCache(String quadKey, RangeQueryEstimate est,
												double ratio) throws IOException {
		String geomColName = m_ds.getGeometryColumn();
		FStream<Record> matcheds = m_cache.get(m_dsId, quadKey)
											.fstream()
											.filter(r -> {
												Geometry geom = r.getGeometry(geomColName);
												return m_pkey.intersects(geom);
											});
		if ( ratio < 1 ) {
			// quadKey에 해당하는 파티션에 샘플링할 레코드 수를 계산하고
			// 이 수만큼의 레코드만 추출하도록 연산을 추가한다.
			int matchCount = est.getClusterEstimate(quadKey).get().getMatchCount();
			int takeCount = (int)Math.max(1, Math.round(matchCount * ratio * 1.05));
			matcheds = matcheds.take(takeCount);
		}
		
		return matcheds;
	}
	
	private StartableExecution<Void> forkClusterPrefetcher(RangeQueryEstimate est) {
		FStream<StartableExecution<?>> strm
								= FStream.from(est.getClusterEstimates())
										.map(ClusterEstimate::getQuadKey)
										.filter(qkey -> !m_cache.exists(m_dsId, qkey))
										.takeTopK(5, (k1,k2) -> k2.length() - k1.length())
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
				s_logger.warn("fails to prefetch: ds={}, quadkey={}, cause={}", m_dsId, m_quadKey, e);
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