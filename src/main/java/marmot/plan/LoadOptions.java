package marmot.plan;

import marmot.proto.optor.LoadOptionsProto;
import marmot.support.PBSerializable;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadOptions implements PBSerializable<LoadOptionsProto> {
	public static final LoadOptions DEFAULT = new LoadOptions(FOption.empty());
	
	private final FOption<Integer> m_nsplits;
	
	private LoadOptions(FOption<Integer> splitCount) {
		m_nsplits = splitCount;
	}
	
	public static LoadOptions SPLIT_COUNT(int count) {
		return new LoadOptions(FOption.of(count));
	}
	
	public FOption<Integer> splitCount() {
		return m_nsplits;
	}
	
	public LoadOptions splitCount(int count) {
		return new LoadOptions(FOption.of(count));
	}

	public static LoadOptions fromProto(LoadOptionsProto proto) {
		LoadOptions opts = DEFAULT;
		
		switch ( proto.getOptionalSplitCountPerBlockCase() ) {
			case SPLIT_COUNT_PER_BLOCK:
				opts = opts.splitCount(proto.getSplitCountPerBlock());
				break;
			case OPTIONALSPLITCOUNTPERBLOCK_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return opts;
	}

	@Override
	public LoadOptionsProto toProto() {
		LoadOptionsProto.Builder builder = LoadOptionsProto.newBuilder();
		
		m_nsplits.ifPresent(builder::setSplitCountPerBlock);
		
		return builder.build();
	}

}
