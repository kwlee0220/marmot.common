package marmot.externio.shp;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import picocli.CommandLine.Option;
import utils.UnitUtils;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ShapefileParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private FOption<String> m_typeName = FOption.empty();	// for write
	private FOption<Charset> m_charset = FOption.empty();
	private FOption<String> m_shpSrid = FOption.empty();
	private FOption<Long> m_splitSize = FOption.empty();	// for write
	
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
	
	@Option(names= {"-shp_srid"}, paramLabel="EPSG-code", description="shapefile SRID")
	public ShapefileParameters shpSrid(String srid) {
		m_shpSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> shpSrid() {
		return m_shpSrid;
	}

	public FOption<Long> splitSize() {
		return m_splitSize;
	}

	@Option(names= {"-split_size"}, paramLabel="bytes",
			description="Shapefile split size string (eg. '128mb')")
	public ShapefileParameters splitSize(String splitSize) {
		return splitSize(UnitUtils.parseByteSize(splitSize));
	}
	
	public ShapefileParameters splitSize(long splitSize) {
		Utilities.checkArgument(splitSize > 0, "splitSize > 0");
		
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