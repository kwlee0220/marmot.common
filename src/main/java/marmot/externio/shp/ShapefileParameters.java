package marmot.externio.shp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import picocli.CommandLine.Option;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private FOption<Charset> m_charset = FOption.empty();
	private FOption<String> m_shpSrid = FOption.empty();
	
	public static ShapefileParameters create() {
		return new ShapefileParameters();
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}

	@Option(names={"-charset"}, paramLabel="charset",
			description={"Character encoding of the target shapefile file"})
	public ShapefileParameters charset(String charset) {
		m_charset = FOption.ofNullable(charset)
							.map(Charset::forName);
		return this;
	}
	
	public ShapefileParameters charset(Charset charset) {
		m_charset = FOption.ofNullable(charset);
		return this;
	}
	
	@Option(names= {"-srid"}, paramLabel="EPSG-code", description="shapefile SRID")
	public ShapefileParameters shpSrid(String srid) {
		m_shpSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> shpSrid() {
		return m_shpSrid;
	}
	
	@Override
	public String toString() {
		String srcSrid = m_shpSrid.map(s -> String.format(", srid=%s", s))
									.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}