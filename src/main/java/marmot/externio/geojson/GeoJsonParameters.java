package marmot.externio.geojson;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.vavr.control.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoJsonParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private Option<Charset> m_charset = Option.none();
	private Option<String> m_srcSrid = Option.none();
	
	public static GeoJsonParameters create() {
		return new GeoJsonParameters();
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}
	
	public GeoJsonParameters charset(Charset charset) {
		m_charset = Option.of(charset);
		return this;
	}
	
	public GeoJsonParameters sourceSrid(String srid) {
		m_srcSrid = Option.of(srid);
		return this;
	}
	
	public Option<String> sourceSrid() {
		return m_srcSrid;
	}
	
	@Override
	public String toString() {
		String srcSrid = m_srcSrid.map(s -> String.format(", src_srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}