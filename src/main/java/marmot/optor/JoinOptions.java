package marmot.optor;

import marmot.proto.optor.JoinOptionsProto;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinOptions implements PBSerializable<JoinOptionsProto> {
	public static final JoinOptions INNER_JOIN = new JoinOptions(JoinType.INNER_JOIN, FOption.empty());
	public static final JoinOptions LEFT_OUTER_JOIN = new JoinOptions(JoinType.LEFT_OUTER_JOIN, FOption.empty());
	public static final JoinOptions RIGHT_OUTER_JOIN = new JoinOptions(JoinType.RIGHT_OUTER_JOIN, FOption.empty());
	public static final JoinOptions FULL_OUTER_JOIN = new JoinOptions(JoinType.FULL_OUTER_JOIN, FOption.empty());
	public static final JoinOptions SEMI_JOIN = new JoinOptions(JoinType.SEMI_JOIN, FOption.empty());
	
	private final JoinType m_joinType;
	private final FOption<Integer> m_workerCount;
	
	private JoinOptions(JoinType joinType, FOption<Integer> workerCount) {
		m_joinType = joinType;
		m_workerCount = workerCount;
	}
	
	public static final JoinOptions create(JoinType jtype) {
		return new JoinOptions(jtype, FOption.empty());
	}
	
	public static final JoinOptions INNER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.INNER_JOIN, FOption.of(nworkers));
	}
	
	public static final JoinOptions INNER_JOIN(FOption<Integer> nworkers) {
		return new JoinOptions(JoinType.INNER_JOIN, nworkers);
	}
	
	public static final JoinOptions LEFT_OUTER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.LEFT_OUTER_JOIN, FOption.of(nworkers));
	}
	
	public static final JoinOptions RIGHT_OUTER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.RIGHT_OUTER_JOIN, FOption.of(nworkers));
	}
	
	public static final JoinOptions FULL_OUTER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.FULL_OUTER_JOIN, FOption.of(nworkers));
	}
	
	public static final JoinOptions SEMI_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.SEMI_JOIN, FOption.of(nworkers));
	}
	
	public JoinType joinType() {
		return m_joinType;
	}
	
	public JoinOptions joinType(JoinType joinType) {
		return new JoinOptions(joinType, m_workerCount);
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public JoinOptions workerCount(int count) {
		return new JoinOptions(m_joinType, count > 0 ? FOption.of(count) : FOption.empty());
	}

	public static JoinOptions fromProto(JoinOptionsProto proto) {
		JoinType type = JoinType.fromProto(proto.getJoinType());
		JoinOptions opts = new JoinOptions(type, FOption.empty());
		switch ( proto.getOptionalWorkerCountCase() ) {
			case WORKER_COUNT:
				opts = opts.workerCount(proto.getWorkerCount());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public JoinOptionsProto toProto() {
		JoinOptionsProto.Builder builder = JoinOptionsProto.newBuilder()
												.setJoinType(m_joinType.toProto());
		m_workerCount.ifPresent(cnt -> builder.setWorkerCount(cnt));
		return builder.build();
	}
}
