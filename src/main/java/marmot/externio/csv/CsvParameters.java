package marmot.externio.csv;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import marmot.MarmotInternalException;
import marmot.optor.CsvOptions;
import marmot.optor.StoreAsCsvOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(description="CSV Parameters")
public class CsvParameters {
	private char m_delim = ',';
	private FOption<Character> m_quote = FOption.empty();
	private FOption<Character> m_escape = FOption.empty();
	private FOption<String> m_comment = FOption.empty();
	private FOption<Charset> m_charset = FOption.empty();
	private FOption<Boolean> m_headerFirst = FOption.empty();
	private FOption<String> m_header = FOption.empty();
	private FOption<String> m_pointCols = FOption.empty();
	private FOption<String> m_srid = FOption.empty();
	private FOption<Boolean> m_trimColumns = FOption.empty();
	private FOption<String> m_nullValue = FOption.empty();
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
	
	public FOption<Character> quote() {
		return m_quote;
	}

	@Option(names={"-quote"}, paramLabel="char", description={"quote character for CSV file"})
	public CsvParameters quote(char quote) {
		m_quote = FOption.of(quote);
		return this;
	}
	
	public FOption<Character> escape() {
		return m_escape;
	}

	@Option(names={"-escape"}, paramLabel="char", description={"quote escape character for CSV file"})
	public CsvParameters escape(char escape) {
		m_escape = FOption.of(escape);
		return this;
	}
	
	public FOption<Charset> charset() {
		return m_charset;
	}

	@Option(names={"-charset"}, paramLabel="charset-string",
			description={"Character encoding of the target CSV file"})
	public CsvParameters charset(String charset) {
		m_charset = FOption.ofNullable(charset)
							.map(Charset::forName);
		return this;
	}
	
	public CsvParameters charset(Charset charset) {
		m_charset = FOption.ofNullable(charset);
		return this;
	}
	
	public FOption<String> commentMarker() {
		return m_comment;
	}

	@Option(names={"-comment"}, paramLabel="comment_marker", description={"comment marker"})
	public CsvParameters commentMarker(String comment) {
		m_comment = FOption.ofNullable(comment);
		return this;
	}
	
	public FOption<String> header() {
		return m_header;
	}

	@Option(names={"-header"}, paramLabel="column_list", description={"header field list"})
	public CsvParameters header(String header) {
		Utilities.checkNotNullArgument(header, "CSV header");
		
		m_header = FOption.of(header);
		return this;
	}
	
	public FOption<Boolean> headerFirst() {
		return m_headerFirst;
	}

	@Option(names={"-header_first"}, description="consider the first line as header")
	public CsvParameters headerFirst(boolean flag) {
		m_headerFirst = FOption.of(flag);
		return this;
	}

	@Option(names={"-null_value"}, paramLabel="string",
			description="null value for column")
	public CsvParameters nullValue(String str) {
		m_nullValue = FOption.ofNullable(str);
		return this;
	}
	
	public FOption<String> nullValue() {
		return m_nullValue;
	}

	@Option(names={"-trim_columns"}, description="ignore surrouding spaces")
	public CsvParameters trimColumns(boolean flag) {
		m_trimColumns = FOption.of(flag);
		return this;
	}
	
	public FOption<Boolean> trimColumn() {
		return m_trimColumns;
	}

	@Option(names={"-point_cols"}, paramLabel="xy-columns", description="X,Y columns for point")
	public CsvParameters pointColumns(String pointCols) {
		Utilities.checkNotNullArgument(pointCols, "Point columns are null");
		
		m_pointCols = FOption.ofNullable(pointCols);
		return this;
	}
	
	public FOption<Tuple2<String,String>> pointColumns() {
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
		m_srid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> srid() {
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
		opts = m_quote.transform(opts, (o,q) ->  o.quote(q));
		opts = m_escape.transform(opts, (o,esc) ->  o.quote(esc));
		opts = m_charset.transform(opts, (o,c) ->  o.charset(c));
		opts = m_headerFirst.transform(opts, (o,f) ->  o.headerFirst(f));
		
		return opts;
	}
	
	public CsvOptions toCsvOptions() {
		CsvOptions opts = CsvOptions.DEFAULT(m_delim);
		opts = m_quote.transform(opts, (o,q) ->  o.quote(q));
		opts = m_escape.transform(opts, (o,esc) ->  o.quote(esc));
		opts = m_charset.transform(opts, (o,c) ->  o.charset(c));
		opts = m_headerFirst.transform(opts, (o,f) ->  o.headerFirst(f));
		
		return opts;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst.filter(f -> f).map(f -> ",HF").getOrElse("");
		String nullString = m_nullValue.map(v -> String.format(", null=\"%s\"", v))
										.getOrElse("");
		String csStr = charset().get().toString().toLowerCase();
		csStr = !csStr.equals("utf-8") ? String.format(",%s", csStr) : "";
		String ptStr = pointColumns().map(xy -> String.format(", POINT(%s,%s)", xy._1, xy._2))
									.getOrElse("");
		String srcSrid = m_srid.map(s -> String.format(",%s", s)).getOrElse("");
		return String.format("'%s'%s%s%s%s%s",
								m_delim, headerFirst, ptStr, srcSrid, csStr,
								nullString);
	}
}
