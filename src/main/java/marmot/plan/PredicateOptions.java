package marmot.plan;

import marmot.proto.optor.PredicateOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PredicateOptions implements PBSerializable<PredicateOptionsProto> {
	private FOption<Boolean> m_negated = FOption.empty();
	
	public static PredicateOptions create() {
		return new PredicateOptions();
	}
	
	public FOption<Boolean> negated() {
		return m_negated;
	}
	
	public PredicateOptions negated(boolean flag) {
		m_negated = FOption.of(flag);
		
		return this;
	}

	public static PredicateOptions fromProto(PredicateOptionsProto proto) {
		PredicateOptions opts = PredicateOptions.create();
		
		switch ( proto.getOptionalNegatedCase() ) {
			case NEGATED:
				opts.negated(proto.getNegated());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public PredicateOptionsProto toProto() {
		PredicateOptionsProto.Builder builder = PredicateOptionsProto.newBuilder();
		
		m_negated.ifPresent(builder::setNegated);
		
		return builder.build();
	}

}
