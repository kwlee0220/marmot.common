package marmot.plan;

import marmot.proto.optor.LoadOptionsProto;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadOptions implements PBSerializable<LoadOptionsProto> {
	private FOption<Integer> m_nsplits = FOption.empty();
	
	public static LoadOptions DEFAULT() {
		return new LoadOptions();
	}
	
	public static LoadOptions SPLIT_COUNT(int count) {
		return new LoadOptions().splitCount(count);
	}
	
	public LoadOptions splitCount(int count) {
		Utilities.checkArgument(count >= 1, "count >= 1");
		
		m_nsplits = FOption.of(count);
		return this;
	}
	
	public FOption<Integer> splitCount() {
		return m_nsplits;
	}
	
	public LoadOptions duplicate() {
		LoadOptions opts = LoadOptions.DEFAULT();
		opts.m_nsplits = m_nsplits;
		
		return opts;
	}

	public static LoadOptions fromProto(LoadOptionsProto proto) {
		LoadOptions opts = LoadOptions.DEFAULT();
		
		switch ( proto.getOptionalSplitCountPerBlockCase() ) {
			case SPLIT_COUNT_PER_BLOCK:
				opts.splitCount(proto.getSplitCountPerBlock());
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
