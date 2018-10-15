package marmot.externio.csv;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CsvParameters {
	private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
	
	private CSVFormat m_format = CSVFormat.DEFAULT.withQuote(null);
	private Option<Charset> m_charset = Option.none();
	private boolean m_headerFirst = false;
	private Option<String> m_pointCols = Option.none();
	private Option<String> m_csvSrid = Option.none();
	
	public static CsvParameters create() {
		return new CsvParameters();
	}
	
	public CSVFormat formatForRead() {
		return (m_headerFirst) ? m_format.withFirstRecordAsHeader() : m_format;
	}
	
	public CSVFormat formatForWrite() {
		return (m_headerFirst) ? m_format : m_format.withSkipHeaderRecord();
	}
	
	public Charset charset() {
		return m_charset.getOrElse(DEFAULT_CHARSET);
	}
	
	public CsvParameters charset(Charset charset) {
		m_charset = Option.of(charset);
		return this;
	}
	
	public char delimiter() {
		return m_format.getDelimiter();
	}
	
	public CsvParameters delimiter(Character delim) {
		m_format = m_format.withDelimiter(delim);
		return this;
	}
	
	public Option<Character> quote() {
		return Option.of(m_format.getQuoteCharacter());
	}
	
	public CsvParameters quote(char quote) {
		m_format = m_format.withQuote(quote).withQuoteMode(QuoteMode.MINIMAL);
		return this;
	}
	
	public CsvParameters escape(char escape) {
		m_format = m_format.withEscape(escape);
		return this;
	}
	
	public boolean headerFirst() {
		return m_headerFirst;
	}
	
	public CsvParameters headerFirst(boolean flag) {
		m_headerFirst = flag;
		return this;
	}
	
	public CsvParameters header(String... header) {
		m_format = m_format.withHeader(header);
		return this;
	}
	
	public CsvParameters nullString(String str) {
		m_format = m_format.withNullString(str);
		return this;
	}
	
	public CsvParameters trimField(boolean flag) {
		m_format = m_format.withIgnoreSurroundingSpaces(flag);
		return this;
	}
	
	public CsvParameters pointColumn(String pointCols) {
		Objects.requireNonNull(pointCols, "Point columns are null");
		
		m_pointCols = Option.of(pointCols);
		return this;
	}
	
	public Option<Tuple2<String,String>> pointColumn() {
		return m_pointCols.map(cols -> {
			try {
				CSVFormat format = CSVFormat.DEFAULT.withDelimiter(delimiter());
				quote().forEach(format::withQuote);
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
				throw new MarmotInternalException("" + ignored);
			}
		});
	}
	
	public CsvParameters csvSrid(String srid) {
		m_csvSrid = Option.of(srid);
		return this;
	}
	
	public Option<String> csvSrid() {
		return m_csvSrid;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_format.getSkipHeaderRecord() ? ", header" : "";
		String nullString = (m_format.getNullString() != null)
							? String.format(", null=\"%s\"", m_format.getNullString()) : "";
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		String ptStr = pointColumn().map(xy -> String.format(", POINT(%s,%s)", xy._1, xy._2))
									.getOrElse("");
		String srcSrid = m_csvSrid.map(s -> String.format(", csv_srid=%s", s))
									.getOrElse("");
		return String.format("delim='%s'%s%s%s%s%s",
								m_format.getDelimiter(), headerFirst, ptStr, srcSrid, csStr,
								nullString);
	}
}