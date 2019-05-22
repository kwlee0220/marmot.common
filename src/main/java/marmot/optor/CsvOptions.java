package marmot.optor;

import java.nio.charset.Charset;

import marmot.proto.optor.CsvOptionsProto;
import marmot.support.PBSerializable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(description="CSV options")
public class CsvOptions implements PBSerializable<CsvOptionsProto> {
	private char m_delim = ',';
	private FOption<Character> m_quote = FOption.empty();
	private FOption<Character> m_escape = FOption.empty();
	private FOption<Charset> m_charset = FOption.empty();
	
	public static CsvOptions create() {
		return new CsvOptions();
	}
	
	public char delimiter() {
		return m_delim;
	}

	@Option(names={"-delim"}, paramLabel="char", description={"delimiter for CSV file"})
	public CsvOptions delimiter(Character delim) {
		m_delim = delim;
		return this;
	}
	
	public FOption<Character> quote() {
		return m_quote;
	}

	@Option(names={"-quote"}, paramLabel="char", description={"quote character for CSV file"})
	public CsvOptions quote(char quote) {
		m_quote = FOption.of(quote);
		return this;
	}
	
	public FOption<Character> escape() {
		return m_escape;
	}
	
	public CsvOptions escape(char escape) {
		m_escape = FOption.of(escape);
		return this;
	}
	
	public FOption<Charset> charset() {
		return m_charset;
	}

	@Option(names={"-charset"}, paramLabel="charset-string",
			description={"Character encoding of the target CSV file"})
	public CsvOptions charset(String charset) {
		m_charset = FOption.ofNullable(charset)
							.map(Charset::forName);
		return this;
	}
	
	public CsvOptions charset(Charset charset) {
		m_charset = FOption.ofNullable(charset);
		return this;
	}
	
	public CsvOptions duplicate() {
		CsvOptions dupl = create();
		dupl.m_delim = m_delim;
		dupl.m_charset = m_charset;
		dupl.m_quote = m_quote;
		dupl.m_escape = m_escape;
		
		return dupl;
	}
	
	@Override
	public String toString() {
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		return String.format("delim='%s'%s",
								m_delim, csStr);
	}

	public static CsvOptions fromProto(CsvOptionsProto proto) {
		CsvOptions opts = CsvOptions.create()
									.delimiter(proto.getDelimiter().charAt(0));
		
		switch ( proto.getOptionalQuoteCase() ) {
			case QUOTE:
				opts.quote(proto.getQuote().charAt(0));
				break;
			default:
		}
		switch ( proto.getOptionalEscapeCase() ) {
			case ESCAPE:
				opts.escape(proto.getEscape().charAt(0));
				break;
			default:
		}
		switch ( proto.getOptionalCharsetCase() ) {
			case CHARSET:
				opts.charset(proto.getCharset());
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
		
		return builder.build();
	}

}
