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
		/** 플랜 기반 분석 */
		PLAN,
		/** 모듈 기반 분석 */
		MODULE,
		/** 복합 분석 */
		COMPOSITE,
		/** 시스템 기능 */
		SYSTEM,
		/** 외부 프로그램 기반 분석 */
		EXTERN,
	}
	
	protected MarmotAnalysis(String id, Type type) {
		m_id = id;
		m_type = type;
	}
	
	/**
	 * 분석 식별자를 반환한다.
	 * 
	 * @return	분석 식별자
	 */
	public String getId() {
		return m_id;
	}
	
	/**
	 * 분석 타입을 반환한다.
	 * 
	 * @return	분석 타입
	 */
	public Type getType() {
		return m_type;
	}
	
	@Override
	public boolean equals(Object obj) {
		if ( this == obj ) {
			return true;
		}
		else if ( obj == null || !(obj instanceof MarmotAnalysis) ) {
			return false;
		}
		
		MarmotAnalysis other = (MarmotAnalysis)obj;
		return m_id.equals(other.m_id);
	}
	
	@Override
	public int hashCode() {
		return m_id.hashCode();
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
			case EXTERN_EXEC:
				return ExternAnalysis.fromProto(proto);
			default:
				throw new AssertionError();
		}
	}
}
