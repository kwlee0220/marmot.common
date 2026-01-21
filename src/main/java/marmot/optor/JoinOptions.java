package marmot.optor;

import java.util.Optional;

import utils.Utilities;

import marmot.proto.optor.JoinOptionsProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JoinOptions implements PBSerializable<JoinOptionsProto> {
	public static final JoinOptions INNER_JOIN = new JoinOptions(JoinType.INNER_JOIN, Optional.empty());
	public static final JoinOptions LEFT_OUTER_JOIN = new JoinOptions(JoinType.LEFT_OUTER_JOIN, Optional.empty());
	public static final JoinOptions RIGHT_OUTER_JOIN = new JoinOptions(JoinType.RIGHT_OUTER_JOIN, Optional.empty());
	public static final JoinOptions FULL_OUTER_JOIN = new JoinOptions(JoinType.FULL_OUTER_JOIN, Optional.empty());
	public static final JoinOptions SEMI_JOIN = new JoinOptions(JoinType.SEMI_JOIN, Optional.empty());
	
	private final JoinType m_joinType;
	private final Optional<Integer> m_workerCount;
	
	private JoinOptions(JoinType joinType, Optional<Integer> workerCount) {
		m_joinType = joinType;
		m_workerCount = workerCount;
	}
	
	public static final JoinOptions create(JoinType jtype) {
		return new JoinOptions(jtype, Optional.empty());
	}
	
	public static final JoinOptions INNER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.INNER_JOIN, Optional.of(nworkers));
	}
	
	public static final JoinOptions INNER_JOIN(Optional<Integer> nworkers) {
		return new JoinOptions(JoinType.INNER_JOIN, nworkers);
	}
	
	public static final JoinOptions LEFT_OUTER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.LEFT_OUTER_JOIN, Optional.of(nworkers));
	}
	
	public static final JoinOptions RIGHT_OUTER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.RIGHT_OUTER_JOIN, Optional.of(nworkers));
	}
	
	public static final JoinOptions FULL_OUTER_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.FULL_OUTER_JOIN, Optional.of(nworkers));
	}
	
	public static final JoinOptions SEMI_JOIN(int nworkers) {
		Utilities.checkArgument(nworkers > 0, "nworkers > 0");
		return new JoinOptions(JoinType.SEMI_JOIN, Optional.of(nworkers));
	}
	
	public JoinType joinType() {
		return m_joinType;
	}
	
	public JoinOptions joinType(JoinType joinType) {
		return new JoinOptions(joinType, m_workerCount);
	}
	
	public Optional<Integer> workerCount() {
		return m_workerCount;
	}
	
	public JoinOptions workerCount(int count) {
		return new JoinOptions(m_joinType, count > 0 ? Optional.of(count) : Optional.empty());
	}

	public static JoinOptions fromProto(JoinOptionsProto proto) {
		JoinType type = JoinType.fromProto(proto.getJoinType());
		JoinOptions opts = new JoinOptions(type, Optional.empty());
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
