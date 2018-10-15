package marmot.plan;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

import io.vavr.control.Option;
import marmot.proto.optor.PredicateOptionsProto;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class PredicateOption {
	public static final NegatedOption NEGATED = new NegatedOption();
	
	public abstract void set(PredicateOptionsProto.Builder builder);
	
	public static Option<PredicateOptionsProto>
	toPredicateOptionsProto(PredicateOption[] opts) {
		return toPredicateOptionsProto(Arrays.asList(opts));
	}
	
	public static Option<PredicateOptionsProto> toPredicateOptionsProto(
												List<? extends PredicateOption> opts) {
		Objects.requireNonNull(opts, "SpatialRelationOption list is null");
		
		List<PredicateOption> matcheds = FStream.of(opts)
													.castSafely(PredicateOption.class)
													.toList();
		if ( matcheds.size() > 0 ) {
			PredicateOptionsProto proto
						= FStream.of(matcheds)
								.collectLeft(PredicateOptionsProto.newBuilder(),
											(b,o) -> o.set(b))
								.build();
			return Option.some(proto);
		}
		else {
			return Option.none();
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
