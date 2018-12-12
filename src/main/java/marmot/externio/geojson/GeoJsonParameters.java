package marmot.externio.geojson;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoJsonParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private FOption<Charset> m_charset = FOption.empty();
	private FOption<String> m_srcSrid = FOption.empty();
	
	public static GeoJsonParameters create() {
		return new GeoJsonParameters();
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}
	
	public GeoJsonParameters charset(Charset charset) {
		m_charset = FOption.ofNullable(charset);
		return this;
	}
	
	public GeoJsonParameters sourceSrid(String srid) {
		m_srcSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> sourceSrid() {
		return m_srcSrid;
	}
	
	@Override
	public String toString() {
		String srcSrid = m_srcSrid.map(s -> String.format(", src_srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}