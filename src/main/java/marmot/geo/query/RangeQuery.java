package marmot.geo.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.RecordSet;
import utils.StopWatch;
import utils.Throwables;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RangeQuery {
	private static final Logger s_logger = LoggerFactory.getLogger(RangeQuery.class);
	
	private final String m_dsId;
	private final DataSet m_ds;
	private final Envelope m_range;
	private long m_sampleCount;
	private int m_maxLocalCacheCost;
	private final DataSetPartitionCache m_cache;
	private boolean m_usePrefetch = false;
	
	RangeQuery(DataSet ds, Envelope range, long sampleCount, DataSetPartitionCache cache,
				boolean usePrefetch, int maxLocalCacheCost) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		Utilities.checkNotNullArgument(range, "query range");
		Utilities.checkNotNullArgument(cache, "DataSetPartitionCache");
		Utilities.checkArgument(sampleCount > 0, "SampleCount > 0, but " + sampleCount);
		Utilities.checkArgument(maxLocalCacheCost > 0, "MaxLocalCacheCost > 0, but " + maxLocalCacheCost);
		
		m_ds = ds;
		m_dsId = ds.getId();
		m_range = range;
		m_sampleCount = sampleCount;
		m_cache = cache;
		m_usePrefetch = usePrefetch;
		m_maxLocalCacheCost = maxLocalCacheCost;
	}
	
	/**
	 * 영역질의의 샘플 갯수를 설정한다.
	 * 
	 * @param count	샘플 갯수
	 * @return	질의 영역 객체 (Fluent Interface 구성용)
	 */
	public RangeQuery setSampleCount(long count) {
		Utilities.checkArgument(count > 0, "SampleCount > 0, but " + count);
		
		m_sampleCount = count;
		return this;
	}
	
	/**
	 * 사전 데이터세트 적재 여부를 설정한다.
	 * 
	 * @param flag	사전 적재 여부
	 * @return	질의 영역 객체 (Fluent Interface 구성용)
	 */
	public RangeQuery setUsePrefetch(boolean flag) {
		m_usePrefetch = flag;
		return this;
	}

	/**
	 * 지역 캐쉬 활용 비용 최대 값를 설정한다.
	 * 
	 * @param cost	최대 비용
	 * @return	질의 영역 객체 (Fluent Interface 구성용)
	 */
	public RangeQuery setMaxLocalCacheCost(int cost) {
		Utilities.checkArgument(cost > 0, "MaxLocalCacheCost > 0, but " + cost);
		
		m_maxLocalCacheCost = cost;
		return this;
	}
	
	/**
	 * 영역 질의에 수행한 결과 레코드 세트를 반환한다.
	 * 
	 * @return	레코드 세트 객체
	 */
	public RecordSet run() {
		StopWatch watch = StopWatch.start();
		try {
			// 질의 영역이 DataSet 전체 영역보다 더 넓은 경우는 인덱스를 사용하는 방법보다
			// 그냥 전체를 읽는 방식을 사용한다.
			// 이때 thumbnail이 존재하는 경우에는 이것을 사용하고, 그렇지 않는 경우에만
			// full scan을 사용한다.
			//
			if ( m_range.contains(m_ds.getBounds()) ) {
				if ( m_ds.hasThumbnail() && m_sampleCount > 0 ) {
					s_logger.info("RANGE > DS, use thumbnail scan: id={}", m_dsId);
					return ThumbnailScan.on(m_ds, m_range, m_sampleCount).run();
				}
				else {
					s_logger.info("RANGE > DS, use full scan: id={}, nsamples={}",
									m_dsId, m_sampleCount);
					return FullScan.on(m_ds).setSampleCount(m_sampleCount).run();
				}
			}
			
			// 대상 DataSet에 인덱스가 걸려있지 않는 경우에도 full scan 방식을 사용한다.
			if ( !m_ds.isSpatiallyClustered() ) {
				if ( m_ds.hasThumbnail() && m_sampleCount > 0 ) {
					s_logger.info("no spatial index, try to use mixed(thumbnail/full) scan: id={}", m_dsId);
					return ThumbnailScan.on(m_ds, m_range, m_sampleCount).run();
				}
				else {
					return FullScan.on(m_ds)
									.setRange(m_range)
									.setSampleCount(m_sampleCount)
									.run();
				}
			}
			else {
				// 질의 영역과 겹치는 quad-key들과, 해당 결과 레코드의 수를 추정하여
				// 그에 따른 질의처리를 시도한다.
				return IndexScan.on(m_ds, m_range, m_sampleCount, m_cache, m_maxLocalCacheCost)
								.usePrefetch(m_usePrefetch)
								.run();
			}
		}
		catch ( Throwable e ) {
			throw Throwables.toRuntimeException(Throwables.unwrapThrowable(e));
		}
		finally {
			s_logger.debug("RangeQuery: dataset={}, elapsed={}", m_dsId, watch.stopAndGetElpasedTimeString());
		}
	}
}
