package marmot.geo.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.SpatialClusterInfo;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;
import utils.stream.KVFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RangeQueryEstimate {
	private final DataSet m_ds;
	private final Map<String,MatchedClusterEstimate> m_clusterEstimates;
	private final Envelope m_range;
	private final long m_totalMatchCount;
	
	public static RangeQueryEstimate about(DataSet ds, Envelope range) {
		return new RangeQueryEstimate(ds, range);
	}
	
	private RangeQueryEstimate(DataSet ds, Envelope range) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		Utilities.checkNotNullArgument(range, "query ranage");
		
		m_ds = ds;
		m_range = range;
		
		// 질의 영역과 겹치는 quad-key들과, 추정되는 결과 레코드의 수를 계산한다.
		m_clusterEstimates = estimate();
		m_totalMatchCount = KVFStream.from(m_clusterEstimates)
									.toValueStream()
									.mapToInt(m -> m.m_matchCount)
									.sum();
	}
	
	public long getMatchCountEstimate() {
		return m_totalMatchCount;
	}
	
	public int getMatchingClusterCount() {
		return m_clusterEstimates.size();
	}
	
	public Set<String> getMatchingClusterKeys() {
		return m_clusterEstimates.keySet();
	}
	
	public SpatialClusterInfo getMatchingClusterInfo(String quadKey) {
		return getMatchingCluster(quadKey)
				.map(m -> m.m_info)
				.getOrNull();
	}
	
	public int getMatchCountEstimate(String quadKey) {
		return getMatchingCluster(quadKey)
				.map(m -> m.m_matchCount)
				.getOrElse(0);
	}
	
	@Override
	public String toString() {
		String matchStr = KVFStream.from(m_clusterEstimates).toValueStream().join(",", "[", "]");
		return String.format("%s:%d:%s", m_ds.getId(), m_totalMatchCount, matchStr);
	}
	
	private FOption<MatchedClusterEstimate> getMatchingCluster(String quadKey) {
		return FOption.ofNullable(m_clusterEstimates.get(quadKey));
	}
	
	private class MatchedClusterEstimate {
		private final SpatialClusterInfo m_info;
		private final double m_matchRatio;
		private final int m_matchCount;
		
		private MatchedClusterEstimate(SpatialClusterInfo info, Envelope range) {
			m_info = info;
			
			Envelope clusterArea = info.getTileBounds().intersection(info.getDataBounds());
			Envelope matchingArea = m_range.intersection(clusterArea);
			m_matchRatio = matchingArea.getArea() / clusterArea.getArea();
			m_matchCount = (int)Math.round(m_info.getOwnedRecordCount() * m_matchRatio);
		}
		
		private int getThumbnailRecordCount(double ratio) {
			return (int)Math.round(m_matchCount * ratio);
		}
		
		@Override
		public String toString() {
			return String.format("%s: %d(%.3f)", m_info.getQuadKey(),
									m_info.getOwnedRecordCount(), m_matchRatio);
		}
	}
	
	private Map<String,MatchedClusterEstimate> estimate() {
		List<SpatialClusterInfo> infos = m_ds.querySpatialClusterInfo(m_range);
		return FStream.from(infos)
						.map(info -> new MatchedClusterEstimate(info, m_range))
						.toMap(m -> m.m_info.getQuadKey());
	}
}