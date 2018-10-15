package marmot.optor;

import java.util.Objects;

import marmot.proto.optor.JoinTypeProto;
import marmot.support.ProtoBufEnumSerializable;

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
		Objects.requireNonNull(str, "JoinType string is null");
		
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
