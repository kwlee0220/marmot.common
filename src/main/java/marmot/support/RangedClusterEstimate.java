package marmot.support;

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
public class RangedClusterEstimate {
	private final DataSet m_ds;
	private final Map<String,Match> m_matches;
	private final Envelope m_range;
	private final int m_totalGuess;
	
	public static RangedClusterEstimate about(DataSet ds, Envelope range) {
		return new RangedClusterEstimate(ds, range);
	}
	
	private RangedClusterEstimate(DataSet ds, Envelope range) {
		Utilities.checkNotNullArgument(ds, "DataSet");
		Utilities.checkNotNullArgument(range, "query ranage");
		
		m_ds = ds;
		m_range = range;
		
		// 질의 영역과 겹치는 quad-key들과, 추정되는 결과 레코드의 수를 계산한다.
		m_matches = guessRelevants();
		m_totalGuess = (int)KVFStream.of(m_matches)
									.toValueStream()
									.mapToInt(m -> m.m_matchCount)
									.sum();
	}
	
	public int getTotalMatchCount() {
		return m_totalGuess;
	}
	
	public int getMatchingClusterCount() {
		return m_matches.size();
	}
	
	public Set<String> getMatchingClusterKeys() {
		return m_matches.keySet();
	}
	
	public SpatialClusterInfo getClusterInfo(String quadKey) {
		return getMatch(quadKey)
				.map(m -> m.m_info)
				.getOrNull();
	}
	
	public int getMatchingRecordCount(String quadKey) {
		return getMatch(quadKey)
				.map(m -> m.m_matchCount)
				.getOrElse(0);
	}
	
	private FOption<Match> getMatch(String quadKey) {
		return FOption.ofNullable(m_matches.get(quadKey));
	}
	
	private class Match {
		private final SpatialClusterInfo m_info;
		private final Envelope m_domain;
		private final double m_inClusterPortion;
		private final int m_matchCount;
		
		private Match(SpatialClusterInfo info, Envelope range) {
			m_info = info;
			
			m_domain = info.getTileBounds()
							.intersection(info.getDataBounds());
			Envelope matchingArea = m_range.intersection(m_domain);
			m_inClusterPortion = matchingArea.getArea() / m_domain.getArea();
			m_matchCount = (int)Math.round(m_info.getOwnedRecordCount() * m_inClusterPortion);
		}
		
		private int getThumbnailRecordCount(double ratio) {
			return (int)Math.round(m_matchCount * ratio);
		}
		
		@Override
		public String toString() {
			return String.format("%s: %d(%.3f)", m_info.getQuadKey(),
									m_info.getOwnedRecordCount(), m_inClusterPortion);
		}
	}
	
	private Map<String,Match> guessRelevants() {
		List<SpatialClusterInfo> infos = m_ds.querySpatialClusterInfo(m_range);
		return FStream.from(infos)
						.map(info -> new Match(info, m_range))
						.toMap(m -> m.m_info.getQuadKey(), m -> m);
	}
}