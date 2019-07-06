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
	private final FOption<Boolean> m_headerFirst;
	private final FOption<Long> m_blockSize;
	
	private StoreAsCsvOptions(CsvOptions csvOpts, FOption<Boolean> headerFirst,
							FOption<Long> blockSize) {
		m_csvOptions = csvOpts;
		m_headerFirst = headerFirst;
		m_blockSize = blockSize;
	}
	
	public static StoreAsCsvOptions DEFAULT() {
		return new StoreAsCsvOptions(CsvOptions.DEFAULT(), FOption.empty(), FOption.empty());
	}
	
	public static StoreAsCsvOptions DEFAULT(char delim) {
		return new StoreAsCsvOptions(CsvOptions.DEFAULT(delim), FOption.empty(), FOption.empty());
	}
	
	public char delimiter() {
		return m_csvOptions.delimiter();
	}

	public StoreAsCsvOptions delimiter(char delim) {
		return new StoreAsCsvOptions(m_csvOptions.delimiter(delim), m_headerFirst, m_blockSize);
	}
	
	public FOption<Character> quote() {
		return m_csvOptions.quote();
	}

	public StoreAsCsvOptions quote(char quote) {
		return new StoreAsCsvOptions(m_csvOptions.quote(quote), m_headerFirst, m_blockSize);
	}
	
	public FOption<Character> escape() {
		return m_csvOptions.escape();
	}
	
	public StoreAsCsvOptions escape(char escape) {
		return new StoreAsCsvOptions(m_csvOptions.escape(escape), m_headerFirst, m_blockSize);
	}
	
	public FOption<Charset> charset() {
		return m_csvOptions.charset();
	}

	public StoreAsCsvOptions charset(String charset) {
		return new StoreAsCsvOptions(m_csvOptions.charset(charset), m_headerFirst, m_blockSize);
	}

	public StoreAsCsvOptions charset(Charset charset) {
		return new StoreAsCsvOptions(m_csvOptions.charset(charset), m_headerFirst, m_blockSize);
	}
	
	public FOption<Boolean> headerFirst() {
		return m_headerFirst;
	}

	public StoreAsCsvOptions headerFirst(boolean flag) {
		return new StoreAsCsvOptions(m_csvOptions, FOption.of(flag), m_blockSize);
	}
	
	public FOption<Long> blockSize() {
		return m_blockSize;
	}

	public StoreAsCsvOptions blockSize(long blkSize) {
		return new StoreAsCsvOptions(m_csvOptions, m_headerFirst, FOption.of(blkSize));
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
		StoreAsCsvOptions opts = new StoreAsCsvOptions(csvOpts, FOption.empty(), FOption.empty());
		
		switch ( proto.getOptionalHeaderFirstCase() ) {
			case HEADER_FIRST:
				opts = opts.headerFirst(proto.getHeaderFirst());
				break;
			default:
		}
		switch ( proto.getOptionalBlockSizeCase() ) {
			case BLOCK_SIZE:
				opts = opts.blockSize(proto.getBlockSize());
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
		m_blockSize.ifPresent(builder::setBlockSize);
		
		return builder.build();
	}

}
