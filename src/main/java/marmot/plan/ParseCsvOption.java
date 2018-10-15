package marmot.plan;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.base.Preconditions;

import marmot.proto.optor.ParseCsvProto.OptionsProto;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class ParseCsvOption {
	public static final ThrowParseError THROW_PARSE_ERROR = new ThrowParseError();
	public static final TrimFieldOption TRIM_FIELD = new TrimFieldOption();
	
	public abstract void set(OptionsProto.Builder builder);
	
	public static HeaderOption HEADER(String[] header) {
		Objects.requireNonNull(header, "header is null");
		Preconditions.checkArgument(header.length > 0, "header is empty"); 
		
		return new HeaderOption(Arrays.asList(header));
	}
	
	public static HeaderOption HEADER(Collection<String> header) {
		Objects.requireNonNull(header, "header is null");
		Preconditions.checkArgument(header.size() > 0, "header is empty"); 
		
		return new HeaderOption(header);
	}
	
	public static EscapeCharacterOption ESCAPE(char escape) {
		return new EscapeCharacterOption(escape);
	}
	
	public static QuoteOption QUOTE(char quote) {
		return new QuoteOption(quote);
	}
	
	public static CommentMarkerOption COMMENT(char marker) {
		return new CommentMarkerOption(marker);
	}
	
	public static NullStringOption NULL_STRING(String str) {
		Objects.requireNonNull(str, "null_string is null");
	
		return new NullStringOption(str);
	}
	
	public static OptionsProto toProto(ParseCsvOption... opts) {
		Objects.requireNonNull(opts, "ParseCsvOptions are null");
		Preconditions.checkArgument(opts.length > 0, "ParseCsvOptions is empty"); 
		
		return FStream.of(opts)
					.collectLeft(OptionsProto.newBuilder(),
								(b,o) -> o.set(b))
					.build();
	}

	public static ParseCsvOption[] fromProto(OptionsProto proto) {
		ParseCsvOption[] opts = new ParseCsvOption[0];

		if ( proto.getHeaderColumnCount() > 0 ) {
			opts = ArrayUtils.add(opts, HEADER(proto.getHeaderColumnList()));
		}
		switch ( proto.getOptionalQuoteCase() ) {
			case QUOTE:
				opts = ArrayUtils.add(opts, QUOTE(proto.getQuote().charAt(0)));
				break;
			default:
		}
		switch ( proto.getOptionalEscapeCase() ) {
			case ESCAPE:
				opts = ArrayUtils.add(opts, QUOTE(proto.getEscape().charAt(0)));
				break;
			default:
		}
		switch ( proto.getOptionalNullStringCase() ) {
			case NULL_STRING:
				opts = ArrayUtils.add(opts, NULL_STRING(proto.getNullString()));
				break;
			default:
		}
		switch ( proto.getOptionalTrimFieldCase() ) {
			case TRIM_FIELD:
				opts = ArrayUtils.add(opts, TRIM_FIELD);
				break;
			default:
		}
		switch ( proto.getOptionalThrowParseErrorCase() ) {
			case THROW_PARSE_ERROR:
				opts = ArrayUtils.add(opts, THROW_PARSE_ERROR);
				break;
			default:
		}
		switch ( proto.getOptionalCommentMarkerCase() ) {
			case COMMENT_MARKER:
				opts = ArrayUtils.add(opts, COMMENT(proto.getCommentMarker().charAt(0)));
				break;
			default:
		}
		
		return opts;
	}
	
	public static class HeaderOption extends ParseCsvOption {
		private final Iterable<String> m_header;
		
		private HeaderOption(Iterable<String> header) {
			m_header = header;
		}
		
		public Iterable<String> get() {
			return m_header;
		}
		
		public void set(OptionsProto.Builder builder) {
			builder.addAllHeaderColumn(m_header);
		}
		
		@Override
		public String toString() {
			return String.format("header=%s", FStream.of(m_header).join(","));
		}
	}
	
	public static class NullStringOption extends ParseCsvOption {
		private final String m_nullString;
		
		private NullStringOption(String nullString) {
			m_nullString = nullString;
		}
		
		public String get() {
			return m_nullString;
		}
		
		public void set(OptionsProto.Builder builder) {
			builder.setNullString(m_nullString);
		}
		
		@Override
		public String toString() {
			return String.format("null_string=%s", m_nullString);
		}
	}
	
	public static class EscapeCharacterOption extends ParseCsvOption {
		private final char m_escape;
		
		private EscapeCharacterOption(char escape) {
			m_escape = escape;
		}
		
		public char get() {
			return m_escape;
		}
		
		public void set(OptionsProto.Builder builder) {
			builder.setEscape("" + m_escape);
		}
		
		@Override
		public String toString() {
			return String.format("escape=%s", m_escape);
		}
	}
	
	public static class QuoteOption extends ParseCsvOption {
		private final char m_quote;
		
		private QuoteOption(char delim) {
			m_quote = delim;
		}
		
		public char get() {
			return m_quote;
		}
		
		public void set(OptionsProto.Builder builder) {
			builder.setQuote("" + m_quote);
		}
		
		@Override
		public String toString() {
			return String.format("quote=%s", m_quote);
		}
	}
	
	public static class CommentMarkerOption extends ParseCsvOption {
		private final char m_marker;
		
		private CommentMarkerOption(char marker) {
			m_marker = marker;
		}
		
		public char get() {
			return m_marker;
		}
		
		public void set(OptionsProto.Builder builder) {
			builder.setCommentMarker("" + m_marker);
		}
		
		@Override
		public String toString() {
			return String.format("comment=%s", m_marker);
		}
	}
	
	public static class ThrowParseError extends ParseCsvOption {
		public void set(OptionsProto.Builder builder) {
			builder.setThrowParseError(true);
		}
		
		@Override
		public String toString() {
			return "throw_parse_error";
		}
	}
	
	public static class TrimFieldOption extends ParseCsvOption {
		public void set(OptionsProto.Builder builder) {
			builder.setTrimField(true);
		}
		
		@Override
		public String toString() {
			return "trim_field";
		}
	}
}
