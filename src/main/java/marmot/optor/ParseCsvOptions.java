package marmot.optor;

import java.nio.charset.Charset;

import marmot.proto.optor.ParseCsvProto.ParseCsvOptionsProto;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ParseCsvOptions implements PBSerializable<ParseCsvOptionsProto> {
	private final CsvOptions m_csvOptions;
	
	private FOption<Boolean> m_headerFirst = FOption.empty();
	private FOption<String> m_header = FOption.empty();
	private FOption<Boolean> m_trimColumns = FOption.empty();
	private FOption<String> m_nullValue = FOption.empty();
	private FOption<Integer> m_maxColLength = FOption.empty();
	private FOption<Boolean> m_throwParseError = FOption.empty();
	
	public static ParseCsvOptions create() {
		return new ParseCsvOptions(CsvOptions.create());
	}
	
	private ParseCsvOptions(CsvOptions csvOpts) {
		m_csvOptions = csvOpts;
	}
	
	public char delimiter() {
		return m_csvOptions.delimiter();
	}

	public ParseCsvOptions delimiter(Character delim) {
		m_csvOptions.delimiter(delim);
		return this;
	}
	
	public FOption<Character> quote() {
		return m_csvOptions.quote();
	}

	public ParseCsvOptions quote(char quote) {
		m_csvOptions.quote(quote);
		return this;
	}
	
	public FOption<Character> escape() {
		return m_csvOptions.escape();
	}
	
	public ParseCsvOptions escape(char escape) {
		m_csvOptions.escape(escape);
		return this;
	}
	
	public FOption<Charset> charset() {
		return m_csvOptions.charset();
	}

	public ParseCsvOptions charset(String charset) {
		m_csvOptions.charset(charset);
		return this;
	}

	public ParseCsvOptions charset(Charset charset) {
		m_csvOptions.charset(charset);
		return this;
	}
	
	public FOption<String> header() {
		return m_header;
	}

	public ParseCsvOptions header(String header) {
		Utilities.checkNotNullArgument(header, "CSV header");
		
		m_header = FOption.of(header);
		return this;
	}
	
	public FOption<Boolean> headerFirst() {
		return m_headerFirst;
	}

	public ParseCsvOptions headerFirst(boolean flag) {
		m_headerFirst = FOption.of(flag);
		return this;
	}

	public ParseCsvOptions nullValue(String str) {
		m_nullValue = FOption.ofNullable(str);
		return this;
	}
	
	public FOption<String> nullValue() {
		return m_nullValue;
	}

	public ParseCsvOptions trimColumns(boolean flag) {
		m_trimColumns = FOption.of(flag);
		return this;
	}
	
	public FOption<Boolean> trimColumn() {
		return m_trimColumns;
	}
	
	public FOption<Integer> maxColumnLength() {
		return m_maxColLength;
	}

	public ParseCsvOptions maxColumnLength(int length) {
		Utilities.checkArgument(length > 0, "length > 0");
		
		m_maxColLength = FOption.of(length);
		return this;
	}
	
	public FOption<Boolean> throwParseError() {
		return m_throwParseError;
	}

	public ParseCsvOptions throwParseError(boolean flag) {
		m_throwParseError = FOption.of(flag);
		return this;
	}
	
	public ParseCsvOptions duplicate() {
		ParseCsvOptions dupl = new ParseCsvOptions(m_csvOptions.duplicate());
		dupl.m_headerFirst = m_headerFirst;
		dupl.m_trimColumns = m_trimColumns;
		dupl.m_header = m_header;
		dupl.m_nullValue = m_nullValue;
		dupl.m_maxColLength = m_maxColLength;
		dupl.m_throwParseError = m_throwParseError;
		
		return dupl;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst.filter(f -> f)
											.map(f -> ", header")
											.getOrElse("");
		String nullString = m_nullValue.map(v -> String.format(", null=\"%s\"", v))
										.getOrElse("");
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		return String.format("delim='%s'%s%s%s",
								delimiter(), headerFirst, csStr, nullString);
	}

	public static ParseCsvOptions fromProto(ParseCsvOptionsProto proto) {
		CsvOptions csvOpts = CsvOptions.fromProto(proto.getCsvOptions());
		ParseCsvOptions opts = new ParseCsvOptions(csvOpts);
		
		switch ( proto.getOptionalHeaderCase() ) {
			case HEADER:
				opts.header(proto.getHeader());
				break;
			default:
		}
		switch ( proto.getOptionalHeaderFirstCase() ) {
			case HEADER_FIRST:
				opts.headerFirst(proto.getHeaderFirst());
				break;
			default:
		}
		switch ( proto.getOptionalTrimColumnCase() ) {
			case TRIM_COLUMN:
				opts.trimColumns(proto.getTrimColumn());
				break;
			default:
		}
		switch ( proto.getOptionalNullValueCase() ) {
			case NULL_VALUE:
				opts.nullValue(proto.getNullValue());
				break;
			default:
		}
		switch ( proto.getOptionalMaxColumnLengthCase() ) {
			case MAX_COLUMN_LENGTH:
				opts.maxColumnLength(proto.getMaxColumnLength());
				break;
			default:
		}
		switch ( proto.getOptionalThrowParseErrorCase() ) {
			case THROW_PARSE_ERROR:
				opts.throwParseError(proto.getThrowParseError());
				break;
			default:
		}
		
		
		return opts;
	}

	@Override
	public ParseCsvOptionsProto toProto() {
		ParseCsvOptionsProto.Builder builder = ParseCsvOptionsProto.newBuilder()
														.setCsvOptions(m_csvOptions.toProto());
		
		m_header.ifPresent(builder::setHeader);
		m_headerFirst.ifPresent(builder::setHeaderFirst);
		m_trimColumns.ifPresent(builder::setTrimColumn);
		m_nullValue.ifPresent(builder::setNullValue);
		m_maxColLength.ifPresent(builder::setMaxColumnLength);
		m_throwParseError.ifPresent(builder::setThrowParseError);
		
		return builder.build();
	}

}
