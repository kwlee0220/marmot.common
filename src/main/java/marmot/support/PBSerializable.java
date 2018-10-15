package marmot.support;

import com.google.protobuf.Message;

import marmot.proto.ProtoBufSerializedProto;
import marmot.proto.SerializedProto;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface PBSerializable<T extends Message> {
	public T toProto();
//	public static U fromProto(T proto);
	
	public default SerializedProto serialize() {
		T proto = toProto();

		ProtoBufSerializedProto serialized = ProtoBufSerializedProto.newBuilder()
										.setObjectClass(this.getClass().getName())
										.setProtoClass(proto.getClass().getName())
										.setSerialized(proto.toByteString())
										.build();
		return SerializedProto.newBuilder()
								.setProtoBuf(serialized)
								.build();
	}
}
