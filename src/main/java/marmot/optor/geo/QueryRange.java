package marmot.optor.geo;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import marmot.plan.PredicateOptions;
import marmot.proto.optor.QueryRangeProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QueryRange implements PBSerializable<QueryRangeProto> {
	private final FOption<Envelope> m_keyBounds;
	private final FOption<Geometry> m_keyGeom;
	private final FOption<String> m_keyDsId;
	private final PredicateOptions m_options;
	
	public static QueryRange of(Envelope bounds) {
		return new QueryRange(FOption.of(bounds), FOption.empty(), FOption.empty(),
								PredicateOptions.DEFAULT);
	}
	
	public static QueryRange of(Geometry keyGeom) {
		return new QueryRange(FOption.empty(), FOption.of(keyGeom), FOption.empty(),
								PredicateOptions.DEFAULT);
	}
	
	public static QueryRange of(String keyDsId) {
		return new QueryRange(FOption.empty(), FOption.empty(), FOption.of(keyDsId),
								PredicateOptions.DEFAULT);
	}
	
	private QueryRange(FOption<Envelope> keyBounds, FOption<Geometry> keyGeom,
						FOption<String> keyDsId, 	PredicateOptions opts) {
		m_keyBounds = keyBounds;
		m_keyGeom = keyGeom;
		m_keyDsId = keyDsId;
		m_options = opts;
	}
	
	public FOption<Envelope> getKeyBounds() {
		return m_keyBounds;
	}
	
	public FOption<Geometry> getKeyGeometry() {
		return m_keyGeom;
	}
	
	public FOption<String> getKeyDataSet() {
		return m_keyDsId;
	}
	
	public PredicateOptions options() {
		return m_options;
	}
	
	public QueryRange options(PredicateOptions opts) {
		return new QueryRange(m_keyBounds, m_keyGeom, m_keyDsId, opts);
	}
	
	@Override
	public String toString() {
		String negatedStr = m_options.negated().getOrElse(false) ? ", negated" : "";
		if ( m_keyBounds.isPresent() ) {
			return String.format("range: bounds=%s%s", m_keyBounds, negatedStr);
		}
		if ( m_keyGeom.isPresent() ) {
			return String.format("range: geom=%s%s", m_keyGeom, negatedStr);
		}
		if ( m_keyDsId.isPresent() ) {
			return String.format("range: key.dsid=%s%s", m_keyDsId, negatedStr);
		}
		
		throw new AssertionError("Should not be here");
	}

	public static QueryRange fromProto(QueryRangeProto proto) {
		QueryRange range;
		switch ( proto.getEitherKeyCase() ) {
			case KEY_BOUNDS:
				Envelope bounds = PBUtils.fromProto(proto.getKeyBounds());
				range = QueryRange.of(bounds);
				break;
			case KEY_GEOMETRY:
				Geometry geom = PBUtils.fromProto(proto.getKeyGeometry());
				range = QueryRange.of(geom);
			case KEY_DATASET:
				range = QueryRange.of(proto.getKeyDataset());
				break;
			default:
				throw new IllegalArgumentException("query key is missing: op=query");
		}

		PredicateOptions opts = PredicateOptions.fromProto(proto.getOptions());
		return range.options(opts);
	}

	@Override
	public QueryRangeProto toProto() {
		QueryRangeProto.Builder builder = QueryRangeProto.newBuilder()
															.setOptions(m_options.toProto());
		builder = m_keyBounds.transform(builder, (b,k) -> b.setKeyBounds(PBUtils.toProto(k)));
		builder = m_keyDsId.transform(builder, (b,k) -> b.setKeyDataset(k));
		
		return builder.build();
	}

}
