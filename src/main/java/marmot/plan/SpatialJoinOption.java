package marmot.plan;

import org.apache.commons.lang3.ArrayUtils;

import marmot.optor.geo.SpatialRelation;
import marmot.proto.optor.SpatialJoinOptionsProto;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialJoinOption {
	public static final NegatedOption NEGATED = new NegatedOption();
	public static final ClusterOuterRecordsOption CLUSTER_OUT_RECORDS = new ClusterOuterRecordsOption();
	
	public abstract void set(SpatialJoinOptionsProto.Builder builder);
	
	public static JoinExprOption WITHIN_DISTANCE(double dist) {
		return new JoinExprOption(SpatialRelation.WITHIN_DISTANCE(dist));
	}
	
	public static JoinExprOption JOIN_EXPR(SpatialRelation expr) {
		return new JoinExprOption(expr);
	}
	
	public static ClusterOuterRecordsOption CLUSTER_OUT_RECORDS(boolean flag) {
		return new ClusterOuterRecordsOption(flag);
	}
	
	public static SpatialJoinOptionsProto toProto(SpatialJoinOption... opts) {
		return FStream.of(opts)
					.collectLeft(SpatialJoinOptionsProto.newBuilder(),
								(b,o) -> o.set(b))
					.build();
	}
	
	public static SpatialJoinOptionsProto toProto(Iterable<SpatialJoinOption> opts) {
		return FStream.from(opts)
					.collectLeft(SpatialJoinOptionsProto.newBuilder(),
								(b,o) -> o.set(b))
					.build();
	}

	public static SpatialJoinOption[] fromProto(SpatialJoinOptionsProto proto) {
		SpatialJoinOption[] opts = new SpatialJoinOption[0];
		
		switch ( proto.getOptionalJoinExprCase() ) {
			case JOIN_EXPR:
				opts = ArrayUtils.add(opts, JOIN_EXPR(SpatialRelation.parse(proto.getJoinExpr())));
				break;
			default:
		}
		switch ( proto.getOptionalClusterOuterRecordsCase() ) {
			case CLUSTER_OUTER_RECORDS:
				ClusterOuterRecordsOption opt = new ClusterOuterRecordsOption(proto.getClusterOuterRecords());
				opts = ArrayUtils.add(opts, opt);
				break;
			default:
		}
		switch ( proto.getOptionalNegatedCase() ) {
			case NEGATED:
				opts = ArrayUtils.add(opts, NEGATED);
				break;
			default:
		}
		
		return opts;
	}
	
	public static class JoinExprOption extends SpatialJoinOption {
		private final SpatialRelation m_joinExpr;
		
		private JoinExprOption(SpatialRelation expr) {
			m_joinExpr = expr;
		}
		
		public SpatialRelation get() {
			return m_joinExpr;
		}
		
		public void set(SpatialJoinOptionsProto.Builder builder) {
			builder.setJoinExpr(m_joinExpr.toStringExpr());
		}
		
		@Override
		public String toString() {
			return String.format("join_expr=%s", m_joinExpr);
		}
	}
	
	public static class ClusterOuterRecordsOption extends SpatialJoinOption {
		private final boolean m_flag;
		
		private ClusterOuterRecordsOption(boolean flag) {
			m_flag = flag;
		}
		
		private ClusterOuterRecordsOption() {
			this(true);
		}
		
		public boolean get() {
			return m_flag;
		}
		
		public void set(SpatialJoinOptionsProto.Builder builder) {
			builder.setClusterOuterRecords(m_flag);
		}
	}
	
	public static class NegatedOption extends SpatialJoinOption {
		public void set(SpatialJoinOptionsProto.Builder builder) {
			builder.setNegated(true);
		}
		
		@Override
		public String toString() {
			return "negated";
		}
	}
}
