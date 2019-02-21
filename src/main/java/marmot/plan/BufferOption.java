package marmot.plan;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.vavr.control.Option;
import marmot.proto.optor.BufferTransformProto.OptionsProto;
import marmot.proto.optor.GeomOpOptionsProto;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class BufferOption extends GeomOpOption {
	public abstract void set(OptionsProto.Builder builder);
	
	public static SegmentCoundOption SEGMENT_COUNT(int count) {
		return new SegmentCoundOption(count);
	}
	
	public static Option<OptionsProto> toBufferOptionsProto(GeomOpOption[] opts) {
		return toBufferOptionsProto(Arrays.asList(opts));
	}
	
	public static Option<OptionsProto> toBufferOptionsProto(
												List<? extends GeomOpOption> opts) {
		Objects.requireNonNull(opts, "GeomOpOption list is null");
		
		List<BufferOption> matcheds = FStream.from(opts)
											.castSafely(BufferOption.class)
											.toList();
		if ( matcheds.size() > 0 ) {
			OptionsProto proto =  FStream.from(matcheds)
											.collectLeft(OptionsProto.newBuilder(),
														(b,o) -> o.set(b))
											.build();
			return Option.some(proto);
		}
		else {
			return Option.none();
		}
	}

	public static List<BufferOption> fromProto(OptionsProto proto) {
		List<BufferOption> opts = Lists.newArrayList();
		
		switch ( proto.getOptionalSegmentCountCase() ) {
			case SEGMENT_COUNT:
				opts.add(SEGMENT_COUNT(proto.getSegmentCount()));
				break;
			default:
		}
		
		return opts;
	}
	
	public static class SegmentCoundOption extends BufferOption {
		private final int m_count;
		
		private SegmentCoundOption(int count) {
			Preconditions.checkArgument(count > 0, "invalid segment count: " + count);
			
			m_count = count;
		}
		
		public int get() {
			return m_count;
		}
		
		@Override
		public void set(OptionsProto.Builder builder) {
			builder.setSegmentCount(m_count);
		}
		
		@Override
		public void set(GeomOpOptionsProto.Builder builder) { }
		
		@Override
		public String toString() {
			return String.format("nsegments=%s", m_count);
		}
	}
}
