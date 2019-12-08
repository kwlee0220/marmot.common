package marmot.geo.query;

import java.util.List;

import com.vividsolutions.jts.geom.Envelope;

import marmot.proto.service.RangeQueryEstimateProto;
import marmot.proto.service.RangeQueryEstimateProto.ClusterEstimateProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RangeQueryEstimate implements PBSerializable<RangeQueryEstimateProto> {
	private final String m_dsId;
	private final Envelope m_range;
	private final List<ClusterEstimate> m_clusterEstimates;
	private long m_matchCount;
	
	public RangeQueryEstimate(String dsId, Envelope range, List<ClusterEstimate> clusterEstimates) {
		m_dsId = dsId;
		m_range = range;
		m_clusterEstimates = clusterEstimates;
		m_matchCount = FStream.from(clusterEstimates)
								.mapToLong(est -> (long)est.m_matchCount)
								.sum();
	}
	
	public long getMatchCount() {
		return m_matchCount;
	}
	
	public List<ClusterEstimate> getClusterEstimates() {
		return m_clusterEstimates;
	}
	
	public FOption<ClusterEstimate> getClusterEstimate(String quadKey) {
		return FStream.from(m_clusterEstimates)
						.findFirst(est -> est.getQuadKey().equals(quadKey));
	}
	
	public static RangeQueryEstimate fromProto(RangeQueryEstimateProto proto) {
		List<ClusterEstimate> clusterEstimates = FStream.from(proto.getClusterEstimateList())
														.map(ClusterEstimate::fromProto)
														.toList();
		return new RangeQueryEstimate(proto.getDsId(), PBUtils.fromProto(proto.getRange()),
										clusterEstimates);	
	}

	@Override
	public RangeQueryEstimateProto toProto() {
		return RangeQueryEstimateProto.newBuilder()
										.setDsId(m_dsId)
										.setRange(PBUtils.toProto(m_range))
										.addAllClusterEstimate(FStream.from(m_clusterEstimates)
																	.map(ClusterEstimate::toProto)
																	.toList())
										.build();
	}
	
	public static class ClusterEstimate implements PBSerializable<ClusterEstimateProto> {
		private final String m_quadKey;
		private final int m_totalCount;
		private final int m_matchCount;
		
		public ClusterEstimate(String quadKey, int totalCount, int matchCount) {
			m_quadKey = quadKey;
			m_totalCount = totalCount;
			m_matchCount = matchCount;
		}
		
		public String getQuadKey() {
			return m_quadKey;
		}
		
		public int getMatchCount() {
			return m_matchCount;
		}
		
		public static ClusterEstimate fromProto(ClusterEstimateProto proto) {
			return new ClusterEstimate(proto.getQuadKey(), proto.getTotalCount(),
										proto.getMatchCount());
		}

		@Override
		public ClusterEstimateProto toProto() {
			return ClusterEstimateProto.newBuilder()
										.setQuadKey(m_quadKey)
										.setTotalCount(m_totalCount)
										.setMatchCount(m_matchCount)
										.build();
		}
		
		@Override
		public String toString() {
			return String.format("%s: %d/%d (%.2f)", m_quadKey, m_matchCount, m_totalCount,
								(double)m_matchCount/m_totalCount);
		}
	}
}
