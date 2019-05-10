package marmot.plan;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import marmot.proto.optor.PredicateOptionsProto;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class PredicateOption {
	public static final NegatedOption NEGATED = new NegatedOption();
	
	public abstract void set(PredicateOptionsProto.Builder builder);
	
	public static FOption<PredicateOptionsProto>
	toPredicateOptionsProto(PredicateOption[] opts) {
		return toPredicateOptionsProto(Arrays.asList(opts));
	}
	
	public static FOption<PredicateOptionsProto> toPredicateOptionsProto(
												List<? extends PredicateOption> opts) {
		Utilities.checkNotNullArgument(opts, "SpatialRelationOption list is null");
		
		List<PredicateOption> matcheds = FStream.from(opts)
													.castSafely(PredicateOption.class)
													.toList();
		if ( matcheds.size() > 0 ) {
			PredicateOptionsProto proto
						= FStream.from(matcheds)
								.collectLeft(PredicateOptionsProto.newBuilder(),
											(b,o) -> o.set(b))
								.build();
			return FOption.of(proto);
		}
		else {
			return FOption.empty();
		}
	}

	public static List<PredicateOption> fromProto(PredicateOptionsProto proto) {
		List<PredicateOption> opts = Lists.newArrayList();
		
		switch ( proto.getOptionalNegatedCase() ) {
			case NEGATED:
				opts.add(NEGATED);
				break;
			default:
		}
		
		return opts;
	}
	
	public static class NegatedOption extends PredicateOption {
		public void set(PredicateOptionsProto.Builder builder) {
			builder.setNegated(true);
		}
		
		@Override
		public String toString() {
			return "negated";
		}
	}
}
