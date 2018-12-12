package marmot.externio.shp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private FOption<String> m_typeName = FOption.empty();
	private FOption<Charset> m_charset = FOption.empty();
	private FOption<String> m_shpSrid = FOption.empty();
	private FOption<Integer> m_splitSize = FOption.empty();
	
	public static ShapefileParameters create() {
		return new ShapefileParameters();
	}
	
	public ShapefileParameters typeName(String name) {
		m_typeName = FOption.ofNullable(name);
		return this;
	}
	
	public FOption<String> typeName() {
		return m_typeName;
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}
	
	public ShapefileParameters charset(Charset charset) {
		m_charset = FOption.ofNullable(charset);
		return this;
	}
	
	public ShapefileParameters shpSrid(String srid) {
		m_shpSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> shpSrid() {
		return m_shpSrid;
	}
	
	public FOption<Integer> splitSize() {
		return m_splitSize;
	}
	
	public ShapefileParameters splitSize(int splitSize) {
		m_splitSize = (splitSize > 0) ? FOption.of(splitSize) : FOption.empty();
		return this;
	}
	
	@Override
	public String toString() {
		String srcSrid = m_shpSrid.map(s -> String.format(", shp_srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}