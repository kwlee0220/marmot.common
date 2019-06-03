package marmot.plan;

import marmot.optor.geo.SpatialRelation;
import marmot.proto.optor.SpatialJoinOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialJoinOptions implements PBSerializable<SpatialJoinOptionsProto> {
	private FOption<String> m_joinExpr = FOption.empty();
	private FOption<Boolean> m_clusterOuter = FOption.empty();
	private FOption<Boolean> m_negated = FOption.empty();
	private FOption<String> m_outputCols = FOption.empty();
	
	public static SpatialJoinOptions create() {
		return new SpatialJoinOptions();
	}
	
	public static SpatialJoinOptions OUTPUT(String outCols) {
		return new SpatialJoinOptions().outputColumns(outCols);
	}
	
	public FOption<String> joinExpr() {
		return m_joinExpr;
	}
	
	public SpatialJoinOptions joinExpr(String expr) {
		m_joinExpr = FOption.ofNullable(expr);
		
		return this;
	}
	
	public SpatialJoinOptions joinExpr(SpatialRelation rel) {
		m_joinExpr = FOption.ofNullable(rel).map(SpatialRelation::toStringExpr);
		
		return this;
	}
	
	public SpatialJoinOptions withinDistance(double dist) {
		m_joinExpr = FOption.of(SpatialRelation.WITHIN_DISTANCE(dist))
							.map(SpatialRelation::toStringExpr);
		
		return this;
	}
	
	public FOption<Boolean> clusterOuterRecords() {
		return m_clusterOuter;
	}
	
	public SpatialJoinOptions clusterOuterRecords(boolean flag) {
		m_clusterOuter = FOption.of(flag);
		
		return this;
	}
	
	public FOption<Boolean> negated() {
		return m_negated;
	}
	
	public SpatialJoinOptions negated(boolean flag) {
		m_negated = FOption.of(flag);
		
		return this;
	}
	
	public FOption<String> outputColumns() {
		return m_outputCols;
	}
	
	public SpatialJoinOptions outputColumns(String outCols) {
		m_outputCols = FOption.ofNullable(outCols);
		
		return this;
	}
	
	public static SpatialJoinOptions fromProto(SpatialJoinOptionsProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.create();
		switch ( proto.getOptionalJoinExprCase() ) {
			case JOIN_EXPR:
				opts.joinExpr(proto.getJoinExpr());
				break;
			default:
		}
		switch ( proto.getOptionalClusterOuterRecordsCase() ) {
			case CLUSTER_OUTER_RECORDS:
				opts.clusterOuterRecords(proto.getClusterOuterRecords());
				break;
			default:
		}
		switch ( proto.getOptionalNegatedCase() ) {
			case NEGATED:
				opts.negated(proto.getNegated());
				break;
			default:
		}
		switch ( proto.getOptionalOutputColumnsCase() ) {
			case OUTPUT_COLUMNS:
				opts.outputColumns(proto.getOutputColumns());
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
}
