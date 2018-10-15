package marmot.externio.shp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.vavr.control.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private Option<String> m_typeName = Option.none();
	private Option<Charset> m_charset = Option.none();
	private Option<String> m_shpSrid = Option.none();
	private Option<Integer> m_splitSize = Option.none();
	
	public static ShapefileParameters create() {
		return new ShapefileParameters();
	}
	
	public ShapefileParameters typeName(String name) {
		m_typeName = Option.of(name);
		return this;
	}
	
	public Option<String> typeName() {
		return m_typeName;
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}
	
	public ShapefileParameters charset(Charset charset) {
		m_charset = Option.of(charset);
		return this;
	}
	
	public ShapefileParameters shpSrid(String srid) {
		m_shpSrid = Option.of(srid);
		return this;
	}
	
	public Option<String> shpSrid() {
		return m_shpSrid;
	}
	
	public Option<Integer> splitSize() {
		return m_splitSize;
	}
	
	public ShapefileParameters splitSize(int splitSize) {
		m_splitSize = (splitSize > 0) ? Option.some(splitSize) : Option.none();
		return this;
	}
	
	@Override
	public String toString() {
		String srcSrid = m_shpSrid.map(s -> String.format(", shp_srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}