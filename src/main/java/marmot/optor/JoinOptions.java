package marmot.optor;

import marmot.proto.optor.JoinOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinOptions implements PBSerializable<JoinOptionsProto> {
	private JoinType m_joinType = JoinType.INNER_JOIN;
	private FOption<Integer> m_workerCount = FOption.empty();
	
	public static final JoinOptions INNER_JOIN() {
		return new JoinOptions().joinType(JoinType.INNER_JOIN);
	}
	
	public static final JoinOptions INNER_JOIN(int nworkers) {
		return new JoinOptions().joinType(JoinType.INNER_JOIN).workerCount(nworkers);
	}
	
	public static final JoinOptions LEFT_OUTER_JOIN() {
		return new JoinOptions().joinType(JoinType.LEFT_OUTER_JOIN);
	}
	
	public static final JoinOptions LEFT_OUTER_JOIN(int nworkers) {
		return new JoinOptions().joinType(JoinType.LEFT_OUTER_JOIN).workerCount(nworkers);
	}
	
	public static final JoinOptions RIGHT_OUTER_JOIN() {
		return new JoinOptions().joinType(JoinType.RIGHT_OUTER_JOIN);
	}
	
	public static final JoinOptions RIGHT_OUTER_JOIN(int nworkers) {
		return new JoinOptions().joinType(JoinType.RIGHT_OUTER_JOIN).workerCount(nworkers);
	}
	
	public static final JoinOptions FULL_OUTER_JOIN() {
		return new JoinOptions().joinType(JoinType.FULL_OUTER_JOIN);
	}
	
	public static final JoinOptions FULL_OUTER_JOIN(int nworkers) {
		return new JoinOptions().joinType(JoinType.FULL_OUTER_JOIN).workerCount(nworkers);
	}
	
	public static final JoinOptions SEMI_JOIN() {
		return new JoinOptions().joinType(JoinType.SEMI_JOIN);
	}
	
	public static final JoinOptions SEMI_JOIN(int nworkers) {
		return new JoinOptions().joinType(JoinType.SEMI_JOIN).workerCount(nworkers);
	}
	
	public JoinType joinType() {
		return m_joinType;
	}
	
	public JoinOptions joinType(JoinType joinType) {
		m_joinType = joinType;
		return this;
	}
	
	public FOption<Integer> workerCount() {
		return m_workerCount;
	}
	
	public JoinOptions workerCount(FOption<Integer> count) {
		m_workerCount = count;
		return this;
	}
	
	public JoinOptions workerCount(int count) {
		m_workerCount = count > 0 ? FOption.of(count) : FOption.empty();
		return this;
	}

	public static JoinOptions fromProto(JoinOptionsProto proto) {
		JoinOptions opts = new JoinOptions()
								.joinType(JoinType.fromProto(proto.getJoinType()));
		switch ( proto.getOptionalWorkerCountCase() ) {
			case WORKER_COUNT:
				opts.workerCount(proto.getWorkerCount());
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
