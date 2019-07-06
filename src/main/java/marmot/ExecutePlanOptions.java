package marmot;

import marmot.proto.service.ExecutePlanOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExecutePlanOptions implements PBSerializable<ExecutePlanOptionsProto> {
	public static final ExecutePlanOptions EMPTY
							= new ExecutePlanOptions(FOption.empty(), FOption.empty());
	public static final ExecutePlanOptions DEFAULT = EMPTY;
	public static final ExecutePlanOptions DISABLE_LOCAL_EXEC
							= new ExecutePlanOptions(FOption.of(true), FOption.empty());
	
	private final FOption<Boolean> m_disableLocalExec;
	private final FOption<String> m_mapOutputCompressCodec;
	
	private ExecutePlanOptions(FOption<Boolean> dle, FOption<String> codec) {
		m_disableLocalExec = dle;
		m_mapOutputCompressCodec = codec;
	}
	
	public ExecutePlanOptions disableLocalExecution(boolean flag) {
		return new ExecutePlanOptions(FOption.of(flag), m_mapOutputCompressCodec);
	}
	
	public FOption<Boolean> disableLocalExecution() {
		return m_disableLocalExec;
	}
	
	public ExecutePlanOptions disableMapOutputCompression() {
		return new ExecutePlanOptions(m_disableLocalExec, FOption.empty());
	}
	
	public ExecutePlanOptions mapOutputCompressionCodec(String codec) {
		return new ExecutePlanOptions(m_disableLocalExec, FOption.of(codec));
	}
	
	public FOption<String> mapOutputCompressionCodec() {
		return m_mapOutputCompressCodec;
	}
	
	public static ExecutePlanOptions fromProto(ExecutePlanOptionsProto proto) {
		ExecutePlanOptions opts = EMPTY;
		
		switch ( proto.getOptionalDisableLocalExecutionCase() ) {
			case DISABLE_LOCAL_EXECUTION:
				opts = opts.disableLocalExecution(proto.getDisableLocalExecution());
				break;
			default:
		}
		switch ( proto.getOptionalMapOutputCompressCodecCase() ) {
			case MAP_OUTPUT_COMPRESS_CODEC:
				opts = opts.mapOutputCompressionCodec(proto.getMapOutputCompressCodec());
				break;
			case OPTIONALMAPOUTPUTCOMPRESSCODEC_NOT_SET:
				opts = opts.disableMapOutputCompression();
				break;
			default:
		}
		
		return opts;
	}

	@Override
	public ExecutePlanOptionsProto toProto() {
		ExecutePlanOptionsProto.Builder builder = ExecutePlanOptionsProto.newBuilder();
		m_disableLocalExec.ifPresent(builder::setDisableLocalExecution);
		m_mapOutputCompressCodec.ifPresent(builder::setMapOutputCompressCodec);
		
		return builder.build();
	}
}
