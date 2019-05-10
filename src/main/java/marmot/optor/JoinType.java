package marmot.optor;

import marmot.proto.optor.JoinTypeProto;
import marmot.support.ProtoBufEnumSerializable;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum JoinType implements ProtoBufEnumSerializable<JoinTypeProto> {
	INNER_JOIN,
	LEFT_OUTER_JOIN,
	RIGHT_OUTER_JOIN,
	FULL_OUTER_JOIN,
	SEMI_JOIN,;
	
	public static JoinType fromString(String str) {
		Utilities.checkNotNullArgument(str, "JoinType string is null");
		
		return JoinType.valueOf(str.toUpperCase());
	}
	
	public static JoinType fromProto(JoinTypeProto proto) {
		return JoinType.valueOf(proto.name());
	}

	@Override
	public JoinTypeProto toProto() {
		return JoinTypeProto.valueOf(name());
	}
}
