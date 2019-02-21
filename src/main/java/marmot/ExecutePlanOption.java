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
	public static final MapOutputCompressCodecOption DISABLE_MAP_OUTPUT_COMPRESS
																= new MapOutputCompressCodecOption("none");
	public static final MapOutputCompressCodecOption COMPRESS_MAP_OUTPUT_BY_SNAPPY
															= new MapOutputCompressCodecOption("snappy");
	
	public abstract void set(ExecutePlanOptionsProto.Builder builder);
	
	public static MapOutputCompressCodecOption MAP_OUTPUT_COMPRESS_CODEC(String codec) {
		return new MapOutputCompressCodecOption(codec);
	}
	
	public static ExecutePlanOptionsProto toProto(ExecutePlanOption... opts) {
		return toProto(Arrays.asList(opts));
	}
	
	public static ExecutePlanOptionsProto toProto(List<? extends ExecutePlanOption> opts) {
		if ( opts.size() == 0 ) {
			return null;
		}
		
		List<ExecutePlanOption> matcheds = FStream.from(opts)
											.castSafely(ExecutePlanOption.class)
											.toList();
		return FStream.from(matcheds)
					.collectLeft(ExecutePlanOptionsProto.newBuilder(), (b,o) -> o.set(b))
					.build();
	}
	
	public static boolean disableLocalExecution(List<ExecutePlanOption> opts) {
		return FStream.from(opts)
						.castSafely(DisableLocalExecOption.class)
						.next()
						.isPresent();
	}
	
	public static MapOutputCompressCodecOption getMapOutputCompressCodec(List<ExecutePlanOption> opts) {
		return FStream.from(opts)
						.castSafely(MapOutputCompressCodecOption.class)
						.next()
						.getOrNull();
	}

	public static List<ExecutePlanOption> fromProto(ExecutePlanOptionsProto proto) {
		List<ExecutePlanOption> opts = Lists.newArrayList();

		switch ( proto.getOptionalDisableLocalExecutionCase() ) {
			case DISABLE_LOCAL_EXECUTION:
				opts.add(DISABLE_LOCAL_EXEC);
				break;
			default:
		}
		switch ( proto.getOptionalMapOutputCompressCodecCase() ) {
			case MAP_OUTPUT_COMPRESS_CODEC:
				opts.add(MAP_OUTPUT_COMPRESS_CODEC(proto.getMapOutputCompressCodec()));
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
	
	public static class MapOutputCompressCodecOption extends ExecutePlanOption {
		private String m_codec;
		
		MapOutputCompressCodecOption(String codec) {
			m_codec = codec;
		}
		
		public String getCodec() {
			return m_codec;
		}
		
		public void set(ExecutePlanOptionsProto.Builder builder) {
			builder.setMapOutputCompressCodec(m_codec);
		}
		
		@Override
		public String toString() {
			return "map_output_compress_codec";
		}
	}
}
