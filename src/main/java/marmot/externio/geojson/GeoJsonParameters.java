package marmot.externio.geojson;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import picocli.CommandLine.Option;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GeoJsonParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private FOption<Charset> m_charset = FOption.empty();
	private FOption<String> m_gjsonSrid = FOption.empty();
	
	public static GeoJsonParameters create() {
		return new GeoJsonParameters();
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}

	@Option(names={"-charset"}, paramLabel="charset",
			description={"Character encoding of the target geojson file"})
	public GeoJsonParameters charset(String charset) {
		m_charset = FOption.ofNullable(charset)
							.map(Charset::forName);
		return this;
	}
	
	public GeoJsonParameters charset(Charset charset) {
		m_charset = FOption.ofNullable(charset);
		return this;
	}

	@Option(names= {"-geojson_srid"}, paramLabel="EPSG-code", description="SRID for GeoJson file")
	public GeoJsonParameters geoJsonSrid(String srid) {
		m_gjsonSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> geoJsonSrid() {
		return m_gjsonSrid;
	}
	
	@Override
	public String toString() {
		String srcSrid = m_gjsonSrid.map(s -> String.format(", src_srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}