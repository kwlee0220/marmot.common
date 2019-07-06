package marmot.plan;

import marmot.proto.optor.PredicateOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PredicateOptions implements PBSerializable<PredicateOptionsProto> {
	private final FOption<Boolean> m_negated;
	
	public static PredicateOptions DEFAULT = new PredicateOptions(FOption.empty());
	public static PredicateOptions NEGATED = new PredicateOptions(FOption.of(true));
	
	private PredicateOptions(FOption<Boolean> negated) {
		m_negated = negated;
	}
	
	public static PredicateOptions NEGATED(boolean flag) {
		return new PredicateOptions(FOption.of(flag));
	}
	
	public FOption<Boolean> negated() {
		return m_negated;
	}
	
	public PredicateOptions negated(boolean flag) {
		return new PredicateOptions(FOption.of(flag));
	}

	public static PredicateOptions fromProto(PredicateOptionsProto proto) {
		switch ( proto.getOptionalNegatedCase() ) {
			case NEGATED:
				return NEGATED(proto.getNegated());
			default:
		}
		
		return DEFAULT;
	}

	@Override
	public PredicateOptionsProto toProto() {
		PredicateOptionsProto.Builder builder = PredicateOptionsProto.newBuilder();
		
		m_negated.ifPresent(builder::setNegated);
		
		return builder.build();
	}

}
