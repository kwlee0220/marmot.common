package marmot.protobuf;

import java.lang.reflect.Method;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.protobuf.Message;

import marmot.Column;
import marmot.RecordSchema;
import marmot.proto.ColumnProto;
import marmot.proto.ProtoBufSerializedProto;
import marmot.proto.RecordSchemaProto;
import marmot.proto.SerializedProto;
import marmot.support.PBException;
import marmot.support.PBSerializable;
import utils.Throwables;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ProtoBufActivator {
	private static final Map<Class<? extends Message>,
							Class<? extends PBSerializable<?>>> BINDINGS = Maps.newHashMap();
	static {
		BINDINGS.put(ColumnProto.class, Column.class);
		BINDINGS.put(RecordSchemaProto.class, RecordSchema.class);
	}
	
	public static <T extends Message,S extends PBSerializable<T>>
	void bind(Class<T> protoCls, Class<S> cls) {
		BINDINGS.put(protoCls, cls);
	}
	
	public static <T extends Message> Object activate(T proto) {
		Class<?> protoCls =  proto.getClass();

		if ( proto instanceof SerializedProto ) {
			return PBUtils.deserialize((SerializedProto)proto);
		}
		else if ( proto instanceof ProtoBufSerializedProto ) {
			return PBUtils.deserialize((ProtoBufSerializedProto)proto);
		}
		
		Class<? extends PBSerializable<?>> cls = BINDINGS.get(protoCls);
		if ( cls == null ) {
			String msg = proto.getClass().getSimpleName() + "{" + proto + "}";
			throw new IllegalArgumentException("unregistered ProtoBuf message: proto=" + msg);
		}
		
		try {
			Method method = cls.getMethod("fromProto", new Class<?>[]{proto.getClass()});
			return method.invoke(null, proto);
		}
		catch ( Throwable e ) {
			String msg = proto.getClass().getSimpleName() + "{" + proto + "}";
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to activate object from proto=" + msg, cause);
		}
	}
}
