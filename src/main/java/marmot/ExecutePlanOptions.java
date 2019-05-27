package marmot;

import marmot.proto.service.ExecutePlanOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExecutePlanOptions implements PBSerializable<ExecutePlanOptionsProto> {
	private FOption<Boolean> m_disableLocalExec = FOption.empty();
	private FOption<String> m_mapOutputCompressCodec = FOption.empty();
	
	public static ExecutePlanOptions create() {
		return new ExecutePlanOptions();
	}
	
	public ExecutePlanOptions disableLocalExecution(boolean flag) {
		m_disableLocalExec = FOption.of(flag);
		return this;
	}
	
	public FOption<Boolean> disableLocalExecution() {
		return m_disableLocalExec;
	}
	
	public ExecutePlanOptions disableMapOutputCompression() {
		m_mapOutputCompressCodec = FOption.empty();
		return this;
	}
	
	public ExecutePlanOptions mapOutputCompressionCodec(String codec) {
		m_mapOutputCompressCodec = FOption.ofNullable(codec);
		return this;
	}
	
	public FOption<String> mapOutputCompressionCodec() {
		return m_mapOutputCompressCodec;
	}
	
	public static ExecutePlanOptions fromProto(ExecutePlanOptionsProto proto) {
		ExecutePlanOptions opts = ExecutePlanOptions.create();
		
		switch ( proto.getOptionalDisableLocalExecutionCase() ) {
			case DISABLE_LOCAL_EXECUTION:
				opts.disableLocalExecution(proto.getDisableLocalExecution());
			default:
		}
		switch ( proto.getOptionalMapOutputCompressCodecCase() ) {
			case MAP_OUTPUT_COMPRESS_CODEC:
				opts.mapOutputCompressionCodec(proto.getMapOutputCompressCodec());
				break;
			case OPTIONALMAPOUTPUTCOMPRESSCODEC_NOT_SET:
				opts.m_mapOutputCompressCodec = FOption.empty();
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
