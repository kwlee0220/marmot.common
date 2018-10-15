package marmot;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import marmot.proto.service.ExecutePlanOptionsProto;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ExecutePlanOption {
	public static final DisableLocalExecOption DISABLE_LOCAL_EXEC = new DisableLocalExecOption();
	
	public abstract void set(ExecutePlanOptionsProto.Builder builder);
	
	public static ExecutePlanOptionsProto toProto(ExecutePlanOption... opts) {
		return toProto(Arrays.asList(opts));
	}
	
	public static ExecutePlanOptionsProto toProto(List<? extends ExecutePlanOption> opts) {
		if ( opts.size() == 0 ) {
			return null;
		}
		
		List<ExecutePlanOption> matcheds = FStream.of(opts)
											.castSafely(ExecutePlanOption.class)
											.toList();
		return FStream.of(matcheds)
					.collectLeft(ExecutePlanOptionsProto.newBuilder(), (b,o) -> o.set(b))
					.build();
	}
	
	public static boolean disableLocalExecution(List<ExecutePlanOption> opts) {
		return FStream.of(opts)
						.castSafely(DisableLocalExecOption.class)
						.first()
						.isDefined();
	}

	public static List<ExecutePlanOption> fromProto(ExecutePlanOptionsProto proto) {
		List<ExecutePlanOption> opts = Lists.newArrayList();

		switch ( proto.getOptionalDisableLocalExecutionCase() ) {
			case DISABLE_LOCAL_EXECUTION:
				opts.add(DISABLE_LOCAL_EXEC);
				break;
			default:
		}
		
		return opts;
	}
	
	public static class DisableLocalExecOption extends ExecutePlanOption {
		@Override
		public void set(ExecutePlanOptionsProto.Builder builder) {
			builder.setDisableLocalExecution(true);
		}
		
		@Override
		public String toString() {
			return "disable_local_exec";
		}
	}
}
