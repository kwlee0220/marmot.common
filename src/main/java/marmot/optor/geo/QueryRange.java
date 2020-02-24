package marmot.optor.geo;

import javax.annotation.Nullable;

import com.vividsolutions.jts.geom.Envelope;

import marmot.plan.PredicateOptions;
import marmot.proto.optor.QueryRangeProto;
import marmot.proto.optor.QueryRangeProto.RangeCase;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QueryRange implements PBSerializable<QueryRangeProto> {
	private final RangeCase m_rangeCase;
	@Nullable private final Envelope m_bounds;
	@Nullable private final String m_rangeDsId;
	private final PredicateOptions m_options;
	
	public static QueryRange of(Envelope bounds) {
		return new QueryRange(RangeCase.BOUNDS, bounds, null, PredicateOptions.DEFAULT);
	}
	
	public static QueryRange fromDataSet(String keyDsId) {
		return new QueryRange(RangeCase.DATASET, null, keyDsId, PredicateOptions.DEFAULT);
	}
	
	private QueryRange(RangeCase rangeCase, Envelope bounds, String rangeDsId, PredicateOptions opts) {
		m_rangeCase = rangeCase;
		m_bounds = bounds;
		m_rangeDsId = rangeDsId;
		m_options = opts;
	}
	
	public RangeCase getRangeCase() {
		return m_rangeCase;
	}
	
	public Envelope getRangeBounds() {
		return (m_rangeCase == RangeCase.BOUNDS) ? m_bounds : null;
	}
	
	public String getRangeKeyDataSet() {
		return (m_rangeCase == RangeCase.DATASET) ? m_rangeDsId : null;
	}
	
	public PredicateOptions options() {
		return m_options;
	}
	
	public QueryRange options(PredicateOptions opts) {
		return new QueryRange(m_rangeCase, m_bounds, m_rangeDsId, opts);
	}
	
	@Override
	public String toString() {
		String negatedStr = m_options.negated().getOrElse(false) ? ", negated" : "";
		switch ( m_rangeCase ) {
			case BOUNDS:
				return String.format("range: bounds=%s%s", m_bounds, negatedStr);
			case DATASET:
				return String.format("range: key.dsid=%s%s", m_rangeDsId, negatedStr);
			default:
				throw new IllegalStateException("invalid range case: " + m_rangeCase);
		}
	}

	public static QueryRange fromProto(QueryRangeProto proto) {
		QueryRange range;
		switch ( proto.getRangeCase() ) {
			case BOUNDS:
				Envelope bounds = PBUtils.fromProto(proto.getBounds());
				range = QueryRange.of(bounds);
				break;
			case DATASET:
				range = QueryRange.fromDataSet(proto.getDataset());
				break;
			default:
				throw new IllegalStateException("invalid range case: " + proto.getRangeCase());
		}

		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		return range.options(opts);
	}

	@Override
	public QueryRangeProto toProto() {
		QueryRangeProto.Builder builder = QueryRangeProto.newBuilder()
														.setOptions(m_options.toProto());

		switch ( m_rangeCase ) {
			case BOUNDS:
				builder = builder.setBounds(PBUtils.toProto(m_bounds));
				break;
			case DATASET:
				builder = builder.setDataset(m_rangeDsId);
				break;
			default:
				throw new IllegalStateException("invalid range case: " + m_rangeCase);
		}
		return builder.build();
	}

}
