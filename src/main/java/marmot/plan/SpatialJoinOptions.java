package marmot.plan;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import marmot.optor.geo.SpatialRelation;
import marmot.proto.optor.SpatialJoinOptionsProto;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialJoinOptions implements PBSerializable<SpatialJoinOptionsProto>, Serializable {
	public static final SpatialJoinOptions EMPTY
							= new SpatialJoinOptions(FOption.empty(), FOption.empty(),
													FOption.empty(), FOption.empty());
	public static final SpatialJoinOptions NEGATED
							= new SpatialJoinOptions(FOption.empty(), FOption.empty(),
													FOption.of(true), FOption.empty());
	
	private final FOption<String> m_joinExpr;
	private final FOption<Boolean> m_clusterOuter;
	private final FOption<Boolean> m_negated;
	private final FOption<String> m_outputCols;
	
	private SpatialJoinOptions(FOption<String> joinExpr, FOption<Boolean> clusterOuter,
								FOption<Boolean> negated, FOption<String> outputCols) {
		m_joinExpr = joinExpr;
		m_clusterOuter = clusterOuter;
		m_negated = negated;
		m_outputCols = outputCols;
	}
	
	public static SpatialJoinOptions OUTPUT(String outCols) {
		Utilities.checkNotNullArgument(outCols, "output columns are null");
		
		return new SpatialJoinOptions(FOption.empty(), FOption.empty(), FOption.empty(),
										FOption.of(outCols));
	}
	
	public static SpatialJoinOptions WITHIN_DISTANCE(double dist) {
		Utilities.checkArgument(dist >= 0, "dist >= 0");
		
		FOption<String> joinExprStr = FOption.of(SpatialRelation.WITHIN_DISTANCE(dist))
											.map(SpatialRelation::toStringExpr);
		return new SpatialJoinOptions(joinExprStr, FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public FOption<String> joinExpr() {
		return m_joinExpr;
	}
	
	public SpatialJoinOptions joinExpr(String expr) {
		Utilities.checkNotNullArgument(expr, "join expression");
		
		return new SpatialJoinOptions(FOption.of(expr), m_clusterOuter, m_negated, m_outputCols);
	}
	
	public SpatialJoinOptions joinExpr(SpatialRelation rel) {
		Utilities.checkNotNullArgument(rel, "join expression");

		return joinExpr(rel.toStringExpr());
	}
	
	public SpatialJoinOptions withinDistance(double dist) {
		Utilities.checkArgument(dist >= 0, "dist >= 0");
		
		return joinExpr(SpatialRelation.WITHIN_DISTANCE(dist));
	}
	
	public FOption<Boolean> clusterOuterRecords() {
		return m_clusterOuter;
	}
	
	public SpatialJoinOptions clusterOuterRecords(boolean flag) {
		return new SpatialJoinOptions(m_joinExpr, FOption.of(flag), m_negated, m_outputCols);
	}
	
	public FOption<Boolean> negated() {
		return m_negated;
	}
	
	public SpatialJoinOptions negated(boolean flag) {
		return new SpatialJoinOptions(m_joinExpr, m_clusterOuter, FOption.of(flag),
										m_outputCols);
	}
	
	public FOption<String> outputColumns() {
		return m_outputCols;
	}
	
	public SpatialJoinOptions outputColumns(String outCols) {
		return new SpatialJoinOptions(m_joinExpr, m_clusterOuter, m_negated,
										FOption.of(outCols));
	}
	
	public static SpatialJoinOptions fromProto(SpatialJoinOptionsProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.EMPTY;
		switch ( proto.getOptionalJoinExprCase() ) {
			case JOIN_EXPR:
				opts = opts.joinExpr(proto.getJoinExpr());
				break;
			default:
		}
		switch ( proto.getOptionalClusterOuterRecordsCase() ) {
			case CLUSTER_OUTER_RECORDS:
				opts = opts.clusterOuterRecords(proto.getClusterOuterRecords());
				break;
			default:
		}
		switch ( proto.getOptionalNegatedCase() ) {
			case NEGATED:
				opts = opts.negated(proto.getNegated());
				break;
			default:
		}
		switch ( proto.getOptionalOutputColumnsCase() ) {
			case OUTPUT_COLUMNS:
				opts = opts.outputColumns(proto.getOutputColumns());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public SpatialJoinOptionsProto toProto() {
		SpatialJoinOptionsProto.Builder builder = SpatialJoinOptionsProto.newBuilder();
		m_joinExpr.ifPresent(builder::setJoinExpr);
		m_clusterOuter.ifPresent(builder::setClusterOuterRecords);
		m_negated.ifPresent(builder::setNegated);
		m_outputCols.ifPresent(builder::setOutputColumns);
		
		return builder.build();
	}
	
	private Object writeReplace() {
		return new SerializationProxy(this);
	}
	
	private void readObject(ObjectInputStream stream) throws InvalidObjectException {
		throw new InvalidObjectException("Use Serialization Proxy instead.");
	}

	private static class SerializationProxy implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final SpatialJoinOptionsProto m_proto;
		
		private SerializationProxy(SpatialJoinOptions opts) {
			m_proto = opts.toProto();
		}
		
		private Object readResolve() {
			return SpatialJoinOptions.fromProto(m_proto);
		}
	}
}
