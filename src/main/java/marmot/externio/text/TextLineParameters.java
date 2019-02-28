package marmot.externio.text;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TextLineParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private String m_glob;
	private FOption<Charset> m_charset = FOption.empty();
	private FOption<String> m_commentMarker = FOption.empty();
	
	public static TextLineParameters parameters() {
		return new TextLineParameters();
	}
	
	public String glob() {
		return m_glob;
	}
	
	public TextLineParameters glob(String pattern) {
		m_glob = pattern;
		return this;
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}
	
	public TextLineParameters charset(Charset charset) {
		m_charset = FOption.ofNullable(charset);
		return this;
	}
	
	public TextLineParameters commentMarker(String marker) {
		m_commentMarker = FOption.ofNullable(marker);
		return this;
	}
	
	public FOption<String> commentMarker() {
		return m_commentMarker;
	}
	
	@Override
	public String toString() {
		String srcSrid = m_commentMarker.map(s -> String.format(", src_srid=%s", s))
								.getOrElse("");
		return String.format("charset=%s%s", charset(), srcSrid);
	}
}