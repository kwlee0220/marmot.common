package marmot.optor;

import java.nio.charset.Charset;
import java.util.Optional;

import utils.Utilities;

import marmot.proto.optor.CsvOptionsProto;
import marmot.support.PBSerializable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvOptions implements PBSerializable<CsvOptionsProto> {
	private final char m_delim;
	private final Optional<Character> m_quote;
	private final Optional<Character> m_escape;
	private final Optional<Charset> m_charset;
	private final Optional<Boolean> m_headerFirst;
	
	private CsvOptions(char delim, Optional<Character> quote, Optional<Character> escape,
						Optional<Charset> charset, Optional<Boolean> headerFirst) {
		m_delim = delim;
		m_quote = quote;
		m_escape = escape;
		m_charset = charset;
		m_headerFirst = headerFirst;
	}
	
	public static CsvOptions DEFAULT() {
		return new CsvOptions(',', Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
	}
	
	public static CsvOptions DEFAULT(char delim) {
		return new CsvOptions(delim, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
	}
	
	public static CsvOptions DEFAULT(char delim, char quote) {
		return new CsvOptions(delim, Optional.of(quote), Optional.empty(), Optional.empty(), Optional.empty());
	}
	
	public static CsvOptions DEFAULT(char delim, char quote, char escape) {
		return new CsvOptions(delim, Optional.of(quote), Optional.of(escape), Optional.empty(), Optional.empty());
	}
	
	public char delimiter() {
		return m_delim;
	}

	public CsvOptions delimiter(char delim) {
		return new CsvOptions(delim, m_quote, m_escape, m_charset, m_headerFirst);
	}
	
	public Optional<Character> quote() {
		return m_quote;
	}

	public CsvOptions quote(char quote) {
		return new CsvOptions(m_delim, Optional.of(quote), m_escape, m_charset, m_headerFirst);
	}
	
	public Optional<Character> escape() {
		return m_escape;
	}
	
	public CsvOptions escape(char escape) {
		return new CsvOptions(m_delim, m_quote, Optional.of(escape), m_charset, m_headerFirst);
	}
	
	public Optional<Charset> charset() {
		return m_charset;
	}

	public CsvOptions charset(String charset) {
		return charset(Charset.forName(charset));
	}
	
	public CsvOptions charset(Charset charset) {
		Utilities.checkNotNullArgument(charset, "Charset is null");
		
		return new CsvOptions(m_delim, m_quote, m_escape, Optional.of(charset), m_headerFirst);
	}
	
	public Optional<Boolean> headerFirst() {
		return m_headerFirst;
	}
	
	public CsvOptions headerFirst(Boolean flag) {
		return new CsvOptions(m_delim, m_quote, m_escape, m_charset, Optional.ofNullable(flag));
	}
	
	@Override
	public String toString() {
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		return String.format("delim='%s'%s", m_delim, csStr);
	}

	public static CsvOptions fromProto(CsvOptionsProto proto) {
		CsvOptions opts = CsvOptions.DEFAULT(proto.getDelimiter().charAt(0));
		
		switch ( proto.getOptionalQuoteCase() ) {
			case QUOTE:
				opts = opts.quote(proto.getQuote().charAt(0));
				break;
			default:
		}
		switch ( proto.getOptionalEscapeCase() ) {
			case ESCAPE:
				opts = opts.escape(proto.getEscape().charAt(0));
				break;
			default:
		}
		switch ( proto.getOptionalCharsetCase() ) {
			case CHARSET:
				opts = opts.charset(proto.getCharset());
				break;
			default:
		}
		switch ( proto.getOptionalHeaderFirstCase() ) {
			case HEADER_FIRST:
				opts = opts.headerFirst(proto.getHeaderFirst());
				break;
			default:
		}
		
		
		return opts;
	}

	@Override
	public CsvOptionsProto toProto() {
		CsvOptionsProto.Builder builder = CsvOptionsProto.newBuilder()
														.setDelimiter(""+m_delim);
		
		m_quote.map(c -> c.toString()).ifPresent(builder::setQuote);
		m_escape.map(c -> c.toString()).ifPresent(builder::setEscape);
		m_charset.map(Charset::name).ifPresent(builder::setCharset);
		m_headerFirst.ifPresent(builder::setHeaderFirst);
		
		return builder.build();
	}

}
