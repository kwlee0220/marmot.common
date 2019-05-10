package marmot.plan;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import marmot.proto.optor.LoadOptionsProto;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class LoadOption {
	public abstract void set(LoadOptionsProto.Builder builder);
	
	public static SplitCountOption SPLIT_COUNT(int count) {
		return new SplitCountOption(count);
	}
	
	public static LoadOptionsProto toProto(LoadOption... opts) {
		return toProto(Arrays.asList(opts));
	}
	
	public static LoadOptionsProto toProto(List<LoadOption> opts) {
		Utilities.checkNotNullArgument(opts, "LoadOption are null");
		
		return FStream.from(opts)
					.collectLeft(LoadOptionsProto.newBuilder(),
								(b,o) -> o.set(b))
					.build();
	}

	public static List<LoadOption> fromProto(LoadOptionsProto proto) {
		List<LoadOption> opts = Lists.newArrayList();
		
		switch ( proto.getOptionalSplitCountPerBlockCase() ) {
			case SPLIT_COUNT_PER_BLOCK:
				opts.add(SPLIT_COUNT(proto.getSplitCountPerBlock()));
				break;
			default:
		}
		
		return opts;
	}
	
	public static class SplitCountOption extends LoadOption {
		private final int m_count;
		
		private SplitCountOption(int count) {
			m_count = count;
		}
		
		public int get() {
			return m_count;
		}
		
		public void set(LoadOptionsProto.Builder builder) {
			builder.setSplitCountPerBlock(m_count);
		}
		
		@Override
		public String toString() {
			return String.format("split_count=%d", m_count);
		}
	}
}
