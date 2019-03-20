package marmot.support;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;

import marmot.DataSet;
import marmot.SpatialClusterInfo;
import utils.stream.FStream;
import utils.stream.KVFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RangedClusterEstimate {
	private static final Logger s_logger = LoggerFactory.getLogger(RangedClusterEstimate.class);
	
	private final DataSet m_ds;
	private final Map<String,Match> m_matches;
	private final Envelope m_range;
	private final int m_totalGuess;
	
	public static RangedClusterEstimate about(DataSet ds, Envelope range) {
		return new RangedClusterEstimate(ds, range);
	}
	
	private RangedClusterEstimate(DataSet ds, Envelope range) {
		Objects.requireNonNull(ds, "DataSet");
		Objects.requireNonNull(range, "query ranage");
		
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
	
	public int getRelevantClusterCount() {
		return m_matches.size();
	}
	
	public Set<String> getRelevantQuadKeys() {
		return m_matches.keySet();
	}
	
	public SpatialClusterInfo getClusterInfo(String quadKey) {
		Match match = m_matches.get(quadKey);
		if ( match == null ) {
			return null;
		}
		else {
			return match.m_info;
		}
	}
	
	public int getRelevantRecordCount(String quadKey) {
		Match match = m_matches.get(quadKey);
		if ( match == null ) {
			return 0;
		}
		else {
			return match.m_matchCount;
		}
	}
	
	public int getMatchCount(String quadKey, int totalCount) {
		Match match = m_matches.get(quadKey);
		if ( match == null ) {
			return 0;
		}
		
		double ratio = (double)match.m_matchCount / m_totalGuess;
		
		return (int)Math.round(ratio * totalCount);
	}
	
	private class Match {
		private final SpatialClusterInfo m_info;
		private final Envelope m_domain;
		private final int m_matchCount;
		
		private Match(SpatialClusterInfo info, Envelope range) {
			m_info = info;
			
			Envelope domain = info.getTileBounds();
			domain = domain.intersection(info.getDataBounds());
			m_domain = domain.intersection(m_ds.getBounds());
			
			double ratio =  range.intersection(m_domain).getArea() / m_domain.getArea();
			m_matchCount = Math.max(1, (int)Math.round(info.getRecordCount() * ratio));
		}
		
		@Override
		public String toString() {
			return String.format("%s: %d/%d", m_info.getQuadKey(), m_matchCount,
									m_info.getRecordCount());
		}
	}
	
	private Map<String,Match> guessRelevants() {
		List<SpatialClusterInfo> infos = m_ds.querySpatialClusterInfo(m_range);
		return FStream.from(infos)
						.map(info -> new Match(info, m_range))
						.toMap(m -> m.m_info.getQuadKey(), m -> m);
	}
}