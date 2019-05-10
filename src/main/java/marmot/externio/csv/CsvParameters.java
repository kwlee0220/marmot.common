package marmot.externio.csv;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import marmot.MarmotInternalException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(description="CSV parsing parameters")
public class CsvParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private FOption<Charset> m_charset = FOption.empty();
	private boolean m_headerFirst = false;
	private char m_delim = ',';
	private FOption<Character> m_quote = FOption.empty();
	private FOption<Character> m_escape = FOption.empty();
	private FOption<String> m_pointCols = FOption.empty();
	private FOption<String> m_csvSrid = FOption.empty();
	private boolean m_trimField = false;
	private FOption<String> m_header = FOption.empty();
	private FOption<String> m_nullValue = FOption.empty();
	private boolean m_tiger = false;
	private FOption<Integer> m_maxColLength = FOption.empty();
	
	public static CsvParameters create() {
		return new CsvParameters();
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
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
	
	public CsvParameters escape(char escape) {
		m_escape = FOption.of(escape);
		return this;
	}
	
	public boolean headerFirst() {
		return m_headerFirst;
	}

	@Option(names={"-header_first"}, description="consider the first line as header")
	public CsvParameters headerFirst(boolean flag) {
		m_headerFirst = flag;
		return this;
	}
	
	public boolean tiger() {
		return m_tiger;
	}

	@Option(names={"-tiger"}, description="use the tiger format")
	public CsvParameters tiger(boolean flag) {
		m_tiger = flag;
		return this;
	}

	@Option(names={"-header"}, paramLabel="column_list", description={"header field list"})
	public CsvParameters headerRecord(String header) {
		Utilities.checkNotNullArgument(header, "CSV header");
		
		m_header = FOption.of(header);
		return this;
	}
	
	public FOption<Integer> maxColumnLength() {
		return m_maxColLength;
	}

	@Option(names={"-max_column_length"}, paramLabel="column_length",
			description={"set maximum column length"})
	public CsvParameters maxColumnLength(int length) {
		Utilities.checkArgument(length > 0, "length > 0");
		
		m_maxColLength = FOption.of(length);
		return this;
	}
	
	public FOption<String> headerRecord() {
		return m_header;
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

	@Option(names={"-trim_field"}, description="ignore surrouding spaces")
	public CsvParameters trimField(boolean flag) {
		m_trimField = flag;
		return this;
	}
	
	public boolean trimField() {
		return m_trimField;
	}

	@Option(names={"-point_col"}, paramLabel="xy-columns", description="X,Y columns for point")
	public CsvParameters pointColumn(String pointCols) {
		Utilities.checkNotNullArgument(pointCols, "Point columns are null");
		
		m_pointCols = FOption.ofNullable(pointCols);
		return this;
	}
	
	public FOption<Tuple2<String,String>> pointColumn() {
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

	@Option(names={"-csv_srid"}, paramLabel="EPSG-code", description="EPSG code for input CSV file")
	public CsvParameters csvSrid(String srid) {
		m_csvSrid = FOption.ofNullable(srid);
		return this;
	}
	
	public FOption<String> csvSrid() {
		return m_csvSrid;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst ? ", header" : "";
		String nullString = m_nullValue.map(v -> String.format(", null=\"%s\"", v))
										.getOrElse("");
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		String ptStr = pointColumn().map(xy -> String.format(", POINT(%s,%s)", xy._1, xy._2))
									.getOrElse("");
		String srcSrid = m_csvSrid.map(s -> String.format(", csv_srid=%s", s))
									.getOrElse("");
		return String.format("delim='%s'%s%s%s%s%s",
								m_delim, headerFirst, ptStr, srcSrid, csStr,
								nullString);
	}
}