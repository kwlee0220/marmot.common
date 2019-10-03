package marmot.exec;

import marmot.proto.service.MarmotAnalysisProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class MarmotAnalysis implements PBSerializable<MarmotAnalysisProto> {
	private final String m_id;
	private final Type m_type;
	
	public enum Type {
		PLAN,
		MODULE,
		COMPOSITE,
		SYSTEM,
	}
	
	protected MarmotAnalysis(String id, Type type) {
		m_id = id;
		m_type = type;
	}
	
	public String getId() {
		return m_id;
	}
	
	public Type getType() {
		return m_type;
	}
	
	public static MarmotAnalysis fromProto(MarmotAnalysisProto proto) {
		switch ( proto.getMemberCase() ) {
			case PLAN_EXEC:
				return PlanAnalysis.fromProto(proto);
			case MODULE_EXEC:
				return ModuleAnalysis.fromProto(proto);
			case COMPOSITE_EXEC:
				return CompositeAnalysis.fromProto(proto);
			case SYSTEM_EXEC:
				return SystemAnalysis.fromProto(proto);
			default:
				throw new AssertionError();
		}
	}
}
