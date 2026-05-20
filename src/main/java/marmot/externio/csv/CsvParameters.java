package marmot.externio.csv;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Optional;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import utils.Preconditions;
import utils.Tuple;
import utils.func.Optionals;

import marmot.MarmotInternalException;
import marmot.optor.CsvOptions;
import marmot.optor.StoreAsCsvOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(description="CSV Parameters")
public class CsvParameters {
	private char m_delim = ',';
	private Optional<Character> m_quote = Optional.empty();
	private Optional<Character> m_escape = Optional.empty();
	private Optional<String> m_comment = Optional.empty();
	private Optional<Charset> m_charset = Optional.empty();
	private Optional<Boolean> m_headerFirst = Optional.empty();
	private Optional<String> m_header = Optional.empty();
	private Optional<String> m_pointCols = Optional.empty();
	private Optional<String> m_srid = Optional.empty();
	private Optional<Boolean> m_trimColumns = Optional.empty();
	private Optional<String> m_nullValue = Optional.empty();
	private boolean m_tiger = false;
	
	public static CsvParameters create() {
		return new CsvParameters();
	}
	
	public char delimiter() {
		return m_delim;
	}

	@Option(names={"-delim"}, paramLabel="char", description={"delimiter for CSV file"})
	public CsvParameters delimiter(Character delim) {
		m_delim = delim;
		return this;
	}
	
	public Optional<Character> quote() {
		return m_quote;
	}

	@Option(names={"-quote"}, paramLabel="char", description={"quote character for CSV file"})
	public CsvParameters quote(char quote) {
		m_quote = Optional.of(quote);
		return this;
	}
	
	public Optional<Character> escape() {
		return m_escape;
	}

	@Option(names={"-escape"}, paramLabel="char", description={"quote escape character for CSV file"})
	public CsvParameters escape(char escape) {
		m_escape = Optional.of(escape);
		return this;
	}
	
	public Optional<Charset> charset() {
		return m_charset;
	}

	@Option(names={"-charset"}, paramLabel="charset-string",
			description={"Character encoding of the target CSV file"})
	public CsvParameters charset(String charset) {
		m_charset = Optional.ofNullable(charset)
							.map(Charset::forName);
		return this;
	}
	
	public CsvParameters charset(Charset charset) {
		m_charset = Optional.ofNullable(charset);
		return this;
	}
	
	public Optional<String> commentMarker() {
		return m_comment;
	}

	@Option(names={"-comment"}, paramLabel="comment_marker", description={"comment marker"})
	public CsvParameters commentMarker(String comment) {
		m_comment = Optional.ofNullable(comment);
		return this;
	}
	
	public Optional<String> header() {
		return m_header;
	}

	@Option(names={"-header"}, paramLabel="column_list", description={"header field list"})
	public CsvParameters header(String header) {
		Preconditions.checkNotNullArgument(header, "CSV header");
		
		m_header = Optional.of(header);
		return this;
	}
	
	public Optional<Boolean> headerFirst() {
		return m_headerFirst;
	}

	@Option(names={"-header_first"}, description="consider the first line as header")
	public CsvParameters headerFirst(boolean flag) {
		m_headerFirst = Optional.of(flag);
		return this;
	}

	@Option(names={"-null_value"}, paramLabel="string",
			description="null value for column")
	public CsvParameters nullValue(String str) {
		m_nullValue = Optional.ofNullable(str);
		return this;
	}
	
	public Optional<String> nullValue() {
		return m_nullValue;
	}

	@Option(names={"-trim_columns"}, description="ignore surrouding spaces")
	public CsvParameters trimColumns(boolean flag) {
		m_trimColumns = Optional.of(flag);
		return this;
	}
	
	public Optional<Boolean> trimColumn() {
		return m_trimColumns;
	}

	@Option(names={"-point_cols"}, paramLabel="xy-columns", description="X,Y columns for point")
	public CsvParameters pointColumns(String pointCols) {
		Preconditions.checkNotNullArgument(pointCols, "Point columns are null");
		
		m_pointCols = Optional.ofNullable(pointCols);
		return this;
	}
	
	public Optional<Tuple<String,String>> pointColumns() {
		return m_pointCols.map(cols -> {
			try {
				CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimiter());
				quote().ifPresent(format::withQuote);
				CSVRecord rec = format.parse(new StringReader(cols))
										.getRecords()
										.get(0);
				if ( rec.size() != 2 ) {
					throw new IllegalArgumentException("invalid point column names='"
													+ cols + "'");
				}
				
				return Tuple.of(rec.get(0), rec.get(1));
			}
			catch ( IOException ignored ) {
				throw new MarmotInternalException("fails to parse 'point_col' paramenter: "
													+ cols + ", cause=" + ignored);
			}
		});
	}

	@Option(names={"-srid"}, paramLabel="EPSG-code", description="EPSG code for input CSV file")
	public CsvParameters srid(String srid) {
		m_srid = Optional.ofNullable(srid);
		return this;
	}
	
	public Optional<String> srid() {
		return m_srid;
	}
	
	public boolean tiger() {
		return m_tiger;
	}

	@Option(names={"-tiger"}, description="use the tiger format")
	public CsvParameters tiger(boolean flag) {
		m_tiger = flag;
		return this;
	}
	
	public CsvParameters duplicate() {
		CsvParameters dupl = create();
		dupl.m_delim = m_delim;
		dupl.m_charset = m_charset;
		dupl.m_headerFirst = m_headerFirst;
		dupl.m_quote = m_quote;
		dupl.m_escape = m_escape;
		dupl.m_comment = m_comment;
		dupl.m_pointCols = m_pointCols;
		dupl.m_srid = m_srid;
		dupl.m_trimColumns = m_trimColumns;
		dupl.m_header = m_header;
		dupl.m_nullValue = m_nullValue;
		dupl.m_tiger = m_tiger;
		
		return dupl;
	}
	
	public StoreAsCsvOptions toStoreOptions() {
		StoreAsCsvOptions opts = StoreAsCsvOptions.DEFAULT(m_delim);
		opts = Optionals.transform(m_quote, opts, (o,q) ->  o.quote(q));
		opts = Optionals.transform(m_escape, opts, (o,q) ->  o.escape(q));
		opts = Optionals.transform(m_charset, opts, (o,q) ->  o.charset(q));
		opts = Optionals.transform(m_headerFirst, opts, (o,q) ->  o.headerFirst(q));
		
		return opts;
	}
	
	public CsvOptions toCsvOptions() {
		CsvOptions opts = CsvOptions.DEFAULT(m_delim);
		opts = Optionals.transform(m_quote, opts, (o,q) ->  o.quote(q));
		opts = Optionals.transform(m_escape, opts, (o,q) ->  o.escape(q));
		opts = Optionals.transform(m_charset, opts, (o,q) ->  o.charset(q));
		opts = Optionals.transform(m_headerFirst, opts, (o,q) ->  o.headerFirst(q));
		
		return opts;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst.filter(f -> f).map(f -> ",HF").orElse("");
		String nullString = m_nullValue.map(v -> String.format(", null=\"%s\"", v))
										.orElse("");
		String csStr = charset().orElseGet(() -> Charset.defaultCharset())
								.toString().toLowerCase();
		csStr = !csStr.equals("utf-8") ? String.format(",%s", csStr) : "";
		String ptStr = pointColumns().map(xy -> String.format(", POINT(%s,%s)", xy._1, xy._2))
									.orElse("");
		String srcSrid = m_srid.map(s -> String.format(",%s", s)).orElse("");
		return String.format("'%s'%s%s%s%s%s",
								m_delim, headerFirst, ptStr, srcSrid, csStr,
								nullString);
	}
}
