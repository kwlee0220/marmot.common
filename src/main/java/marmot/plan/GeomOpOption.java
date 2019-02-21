package marmot.plan;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;

import io.vavr.control.Option;
import marmot.proto.optor.GeomOpOptionsProto;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class GeomOpOption {
	public static final SkipErrorOption SKIP_ERROR = new SkipErrorOption();
	
	public abstract void set(GeomOpOptionsProto.Builder builder);
	
	public static OutGeomColOption OUTPUT(String geomCol) {
		return new OutGeomColOption(geomCol);
	}
	
	public static Option<GeomOpOptionsProto> toGeomOpOptionsProto(GeomOpOption[] opts) {
		return toGeomOpOptionsProto(Arrays.asList(opts));
	}
	
	public static Option<GeomOpOptionsProto> toGeomOpOptionsProto(
												List<? extends GeomOpOption> opts) {
		Objects.requireNonNull(opts, "GeomOpOption list is null");
		
		List<GeomOpOption> matcheds = FStream.from(opts)
											.castSafely(GeomOpOption.class)
											.toList();
		if ( matcheds.size() > 0 ) {
			GeomOpOptionsProto proto =  FStream.from(matcheds)
												.collectLeft(GeomOpOptionsProto.newBuilder(),
															(b,o) -> o.set(b))
												.build();
			return Option.some(proto);
		}
		else {
			return Option.none();
		}
	}

	public static List<GeomOpOption> fromProto(GeomOpOptionsProto proto) {
		List<GeomOpOption> opts = Lists.newArrayList();
		
		switch ( proto.getOptionalOutGeomColCase() ) {
			case OUT_GEOM_COL:
				opts.add(OUTPUT(proto.getOutGeomCol()));
				break;
			default:
		}
		switch ( proto.getOptionalSkipErrorCase() ) {
			case SKIP_ERROR:
				opts.add(SKIP_ERROR);
				break;
			default:
		}
		
		return opts;
	}
	
	public static class OutGeomColOption extends GeomOpOption {
		private final String m_geomCol;
		
		private OutGeomColOption(String geomCol) {
			Objects.requireNonNull(geomCol, "output geometry column is null");
			
			m_geomCol = geomCol;
		}
		
		public String get() {
			return m_geomCol;
		}
		
		public void set(GeomOpOptionsProto.Builder builder) {
			builder.setOutGeomCol(m_geomCol);
		}
		
		@Override
		public String toString() {
			return String.format("out_geom=%s", m_geomCol);
		}
	}
	
	public static class SkipErrorOption extends GeomOpOption {
		public void set(GeomOpOptionsProto.Builder builder) {
			builder.setSkipError(true);
		}
		
		@Override
		public String toString() {
			return "failure_skipped";
		}
	}
}
