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
	private final FOption<Boolean> m_headerFirst;
	private final FOption<String> m_header;
	private final FOption<Boolean> m_trimColumns;
	private final FOption<String> m_nullValue;
	private final FOption<Boolean> m_throwParseError;
	
	private ParseCsvOptions(CsvOptions csvOpts, FOption<Boolean> headerFirst,
							FOption<String> header, FOption<Boolean> trimColumns,
							FOption<String> nullValue, FOption<Boolean> throwParseError) {
		m_csvOptions = csvOpts;
		m_headerFirst = headerFirst;
		m_header = header;
		m_trimColumns = trimColumns;
		m_nullValue = nullValue;
		m_throwParseError = throwParseError;
	}
	
	public static ParseCsvOptions DEFAULT() {
		return new ParseCsvOptions(CsvOptions.DEFAULT(), FOption.empty(), FOption.empty(),
									FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public static ParseCsvOptions DEFAULT(char delim) {
		return new ParseCsvOptions(CsvOptions.DEFAULT(delim), FOption.empty(), FOption.empty(),
									FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public static ParseCsvOptions DEFAULT(char delim, char quote) {
		return new ParseCsvOptions(CsvOptions.DEFAULT(delim, quote), FOption.empty(),
									FOption.empty(), FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public static ParseCsvOptions DEFAULT(char delim, char quote, char escape) {
		return new ParseCsvOptions(CsvOptions.DEFAULT(delim, quote, escape), FOption.empty(),
									FOption.empty(), FOption.empty(), FOption.empty(), FOption.empty());
	}
	
	public char delimiter() {
		return m_csvOptions.delimiter();
	}

	public ParseCsvOptions delimiter(char delim) {
		return new ParseCsvOptions(m_csvOptions.delimiter(delim), m_headerFirst,
									m_header, m_trimColumns, m_nullValue, m_throwParseError);
	}
	
	public FOption<Character> quote() {
		return m_csvOptions.quote();
	}

	public ParseCsvOptions quote(char quote) {
		return new ParseCsvOptions(m_csvOptions.quote(quote), m_headerFirst,
									m_header, m_trimColumns, m_nullValue, m_throwParseError);
	}
	
	public FOption<Character> escape() {
		return m_csvOptions.escape();
	}
	
	public ParseCsvOptions escape(char escape) {
		return new ParseCsvOptions(m_csvOptions.escape(escape), m_headerFirst,
									m_header, m_trimColumns, m_nullValue, m_throwParseError);
	}
	
	public FOption<Charset> charset() {
		return m_csvOptions.charset();
	}

	public ParseCsvOptions charset(String charset) {
		return new ParseCsvOptions(m_csvOptions.charset(charset), m_headerFirst,
									m_header, m_trimColumns, m_nullValue, m_throwParseError);
	}

	public ParseCsvOptions charset(Charset charset) {
		return new ParseCsvOptions(m_csvOptions.charset(charset), m_headerFirst,
									m_header, m_trimColumns, m_nullValue, m_throwParseError);
	}
	
	public FOption<String> header() {
		return m_header;
	}

	public ParseCsvOptions header(String header) {
		Utilities.checkNotNullArgument(header, "CSV header");
		
		return new ParseCsvOptions(m_csvOptions, m_headerFirst, FOption.of(header),
									m_trimColumns, m_nullValue, m_throwParseError);
	}
	
	public FOption<Boolean> headerFirst() {
		return m_headerFirst;
	}

	public ParseCsvOptions headerFirst(boolean flag) {
		return new ParseCsvOptions(m_csvOptions, FOption.of(flag), m_header,
									m_trimColumns, m_nullValue, m_throwParseError);
	}

	public ParseCsvOptions nullValue(String str) {
		return new ParseCsvOptions(m_csvOptions, m_headerFirst, m_header,
									m_trimColumns, FOption.ofNullable(str), m_throwParseError);
	}
	
	public FOption<String> nullValue() {
		return m_nullValue;
	}

	public ParseCsvOptions trimColumns(boolean flag) {
		return new ParseCsvOptions(m_csvOptions, m_headerFirst, m_header,
									FOption.of(flag), m_nullValue, m_throwParseError);
	}
	
	public FOption<Boolean> trimColumn() {
		return m_trimColumns;
	}
	
	public FOption<Boolean> throwParseError() {
		return m_throwParseError;
	}

	public ParseCsvOptions throwParseError(boolean flag) {
		return new ParseCsvOptions(m_csvOptions, m_headerFirst, m_header,
									m_trimColumns, m_nullValue, FOption.of(flag));
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
		ParseCsvOptions opts = new ParseCsvOptions(csvOpts, FOption.empty(), FOption.empty(),
													FOption.empty(), FOption.empty(), FOption.empty());
		
		switch ( proto.getOptionalHeaderCase() ) {
			case HEADER:
				opts = opts.header(proto.getHeader());
				break;
			default:
		}
		switch ( proto.getOptionalHeaderFirstCase() ) {
			case HEADER_FIRST:
				opts = opts.headerFirst(proto.getHeaderFirst());
				break;
			default:
		}
		switch ( proto.getOptionalTrimColumnCase() ) {
			case TRIM_COLUMN:
				opts = opts.trimColumns(proto.getTrimColumn());
				break;
			default:
		}
		switch ( proto.getOptionalNullValueCase() ) {
			case NULL_VALUE:
				opts = opts.nullValue(proto.getNullValue());
				break;
			default:
		}
		switch ( proto.getOptionalThrowParseErrorCase() ) {
			case THROW_PARSE_ERROR:
				opts = opts.throwParseError(proto.getThrowParseError());
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
		m_throwParseError.ifPresent(builder::setThrowParseError);
		
		return builder.build();
	}

}
