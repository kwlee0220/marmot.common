package marmot.support;

import com.google.protobuf.ProtocolMessageEnum;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface ProtoBufEnumSerializable<T extends ProtocolMessageEnum> {
	public T toProto();
//	public static U fromProto(T proto);
}
