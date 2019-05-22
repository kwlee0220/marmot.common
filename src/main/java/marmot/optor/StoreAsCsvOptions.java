package marmot.optor;

import java.nio.charset.Charset;

import marmot.proto.optor.StoreAsCsvProto.StoreAsCsvOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAsCsvOptions implements PBSerializable<StoreAsCsvOptionsProto> {
	private final CsvOptions m_csvOptions;
	
	private FOption<Boolean> m_headerFirst = FOption.empty();
	private FOption<Long> m_blockSize = FOption.empty();
	
	public static StoreAsCsvOptions create() {
		return new StoreAsCsvOptions(CsvOptions.create());
	}
	
	private StoreAsCsvOptions(CsvOptions csvOpts) {
		m_csvOptions = csvOpts;
	}
	
	public char delimiter() {
		return m_csvOptions.delimiter();
	}

	public StoreAsCsvOptions delimiter(Character delim) {
		m_csvOptions.delimiter(delim);
		return this;
	}
	
	public FOption<Character> quote() {
		return m_csvOptions.quote();
	}

	public StoreAsCsvOptions quote(char quote) {
		m_csvOptions.quote(quote);
		return this;
	}
	
	public FOption<Character> escape() {
		return m_csvOptions.escape();
	}
	
	public StoreAsCsvOptions escape(char escape) {
		m_csvOptions.escape(escape);
		return this;
	}
	
	public FOption<Charset> charset() {
		return m_csvOptions.charset();
	}

	public StoreAsCsvOptions charset(String charset) {
		m_csvOptions.charset(charset);
		return this;
	}

	public StoreAsCsvOptions charset(Charset charset) {
		m_csvOptions.charset(charset);
		return this;
	}
	
	public FOption<Boolean> headerFirst() {
		return m_headerFirst;
	}

	public StoreAsCsvOptions headerFirst(boolean flag) {
		m_headerFirst = FOption.of(flag);
		return this;
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public StoreAsCsvOptions blockSize(long blkSize) {
		m_blockSize = FOption.of(blkSize);
		return this;
	}
	
	public StoreAsCsvOptions duplicate() {
		StoreAsCsvOptions dupl = new StoreAsCsvOptions(m_csvOptions.duplicate());
		dupl.m_headerFirst = m_headerFirst;
		
		return dupl;
	}
	
	@Override
	public String toString() {
		String headerFirst = m_headerFirst.filter(f -> f)
											.map(f -> ", header")
											.getOrElse("");
		String csStr = !charset().toString().equalsIgnoreCase("utf-8")
						? String.format(", %s", charset().toString()) : "";
		return String.format("delim='%s'%s%s",
								delimiter(), headerFirst, csStr);
	}

	public static StoreAsCsvOptions fromProto(StoreAsCsvOptionsProto proto) {
		CsvOptions csvOpts = CsvOptions.fromProto(proto.getCsvOptions());
		StoreAsCsvOptions opts = new StoreAsCsvOptions(csvOpts);
		
		switch ( proto.getOptionalHeaderFirstCase() ) {
			case HEADER_FIRST:
				opts.headerFirst(proto.getHeaderFirst());
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public StoreAsCsvOptionsProto toProto() {
		StoreAsCsvOptionsProto.Builder builder = StoreAsCsvOptionsProto.newBuilder()
														.setCsvOptions(m_csvOptions.toProto());
		
		m_headerFirst.ifPresent(builder::setHeaderFirst);
		
		return builder.build();
	}

}
