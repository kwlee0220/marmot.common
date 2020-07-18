package marmot.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import marmot.Record;
import marmot.exec.MarmotExecutionException;
import marmot.geo.GeoClientUtils;
import marmot.proto.BoolProto;
import marmot.proto.CoordinateProto;
import marmot.proto.EnvelopeProto;
import marmot.proto.GeometryProto;
import marmot.proto.IntervalProto;
import marmot.proto.JavaSerializedProto;
import marmot.proto.KeyValueMapProto;
import marmot.proto.KeyValueMapProto.KeyValueProto;
import marmot.proto.PointProto;
import marmot.proto.PropertiesProto;
import marmot.proto.PropertiesProto.PropertyProto;
import marmot.proto.ProtoBufSerializedProto;
import marmot.proto.SerializedProto;
import marmot.proto.Size2dProto;
import marmot.proto.Size2iProto;
import marmot.proto.StringProto;
import marmot.proto.TypeCodeProto;
import marmot.proto.VoidProto;
import marmot.proto.service.BoolResponse;
import marmot.proto.service.DoubleResponse;
import marmot.proto.service.ErrorProto;
import marmot.proto.service.FloatResponse;
import marmot.proto.service.LongResponse;
import marmot.proto.service.MarmotErrorCode;
import marmot.proto.service.RecordResponse;
import marmot.proto.service.StringListResponse;
import marmot.proto.service.StringListResponse.ListProto;
import marmot.proto.service.StringResponse;
import marmot.proto.service.VoidResponse;
import marmot.remote.protobuf.PBMarmotError;
import marmot.support.PBException;
import marmot.support.PBSerializable;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import marmot.type.Interval;
import marmot.type.TypeCode;
import utils.Size2d;
import utils.Size2i;
import utils.Throwables;
import utils.func.CheckedRunnable;
import utils.func.CheckedSupplier;
import utils.func.FOption;
import utils.func.KeyValue;
import utils.io.IOUtils;
import utils.io.Serializables;
import utils.stream.FStream;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBUtils {
	private static final Logger s_logger = LoggerFactory.getLogger(PBUtils.class);
	
	public static final VoidProto VOID = VoidProto.newBuilder().build();
	private static final VoidResponse VOID_RESPONSE = VoidResponse.newBuilder()
																	.setVoid(VOID)
																	.build();

	public static String toJson(MessageOrBuilder proto, boolean omitWs) throws IOException {
		Printer printer = JsonFormat.printer();
		if ( omitWs ) {
			printer = printer.omittingInsignificantWhitespace();
		}
		
		return printer.print(proto);
	}
	
	public static <T extends Message.Builder> T parseJson(String json, T builder) throws IOException {
		JsonFormat.parser().merge(json, builder);
		return builder;
	}
	
	public static boolean isUnavailableException(Throwable cause) {
		if ( cause instanceof StatusRuntimeException ) {
			Status status = ((StatusRuntimeException)cause).getStatus();
			if ( status.getCode() == Status.CANCELLED.getCode() ) {
				return true;
			}
		}
		
		return false;
	}
	
	public static final SerializedProto serialize(Object obj) {
		if ( obj instanceof PBSerializable ) {
			return ((PBSerializable<?>)obj).serialize();
		}
		else if ( obj instanceof Message ) {
			return PBUtils.serialize((Message)obj);
		}
		else if ( obj instanceof Serializable ) {
			return PBUtils.serializeJava((Serializable)obj);
		}
		else {
			throw new IllegalStateException("unable to serialize: " + obj);
		}
	}
	
	public static final SerializedProto serializeJava(Serializable obj) {
		try {
			JavaSerializedProto proto = JavaSerializedProto.newBuilder()
										.setSerialized(ByteString.copyFrom(Serializables.serialize(obj)))
										.build();
			return SerializedProto.newBuilder()
									.setJava(proto)
									.build();
		}
		catch ( Exception e ) {
			throw new PBException("fails to serialize object: proto=" + obj, e);
		}
	}
	
	public static final SerializedProto serialize(Message proto) {
		ProtoBufSerializedProto serialized = ProtoBufSerializedProto.newBuilder()
										.setProtoClass(proto.getClass().getName())
										.setSerialized(proto.toByteString())
										.build();
		return SerializedProto.newBuilder()
								.setProtoBuf(serialized)
								.build();
	}
	
	public static final <T> T deserialize(SerializedProto proto) {
		switch ( proto.getMethodCase() ) {
			case PROTO_BUF:
				return deserialize(proto.getProtoBuf());
			case JAVA:
				return deserialize(proto.getJava());
			default:
				throw new AssertionError("unregistered serialization method: method="
										+ proto.getMethodCase());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(JavaSerializedProto proto) {
		try {
			return (T)Serializables.deserialize(proto.getSerialized().toByteArray());
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto, cause);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static final <T> T deserialize(ProtoBufSerializedProto proto) {
		try {
			ByteString serialized = proto.getSerialized();
			
			FOption<String> clsName = getOptionField(proto, "object_class");
			Class<?> protoCls = Class.forName(proto.getProtoClass());

			Method parseFrom = protoCls.getMethod("parseFrom", ByteString.class);
			Message optorProto = (Message)parseFrom.invoke(null, serialized);
			
			if ( clsName.isPresent() ) {
				Class<?> cls = Class.forName(clsName.get());
				Method fromProto = cls.getMethod("fromProto", protoCls);
				return (T)fromProto.invoke(null, optorProto);
			}
			else {
				return (T)ProtoBufActivator.activate(optorProto);
			}
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto + ", cause=" + cause, cause);
		}
	}
	
	public static Enum<?> getCase(Message proto, String field) {
		try {
			String partName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, field);
			Method getCase = proto.getClass().getMethod("get" + partName + "Case", new Class<?>[0]);
			return (Enum<?>)getCase.invoke(proto, new Object[0]);
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the case " + field, e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> FOption<T> getOptionField(Message proto, String field) {
		try {
			return FOption.ofNullable((T)KVFStream.from(proto.getAllFields())
												.filter(kv -> kv.key().getName().equals(field))
												.next()
												.map(kv -> kv.value())
												.getOrNull());
					}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}

	public static FOption<String> getStringOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(String.class);
	}

	public static FOption<Double> getDoubleOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Double.class);
	}

	public static FOption<Long> geLongOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Long.class);
	}

	public static FOption<Integer> getIntOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Integer.class);
	}

	@SuppressWarnings("unchecked")
	public static <T> T getField(Message proto, String field) {
		try {
			return(T)KVFStream.from(proto.getAllFields())
									.filter(kv -> kv.key().getName().equals(field))
									.next()
									.map(kv -> kv.value())
									.getOrThrow(()
										-> new PBException("unknown field: name=" + field
																	+ ", msg=" + proto));
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}
	
	public static VoidResponse toVoidResponse() {
		return VOID_RESPONSE;
	}
	
	public static VoidResponse toVoidResponse(Throwable e) {
		return VoidResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static StringResponse toStringResponse(Throwable e) {
		return StringResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static StringResponse toStringResponse(String value) {
		return StringResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static StringListResponse toStringListResponse(Iterable<String> values) {
		return StringListResponse.newBuilder()
								.setList(ListProto.newBuilder().addAllValue(values).build())
								.build();
	}
	
	public static StringListResponse toStringListResponse(Throwable e) {
		return StringListResponse.newBuilder()
								.setError(toErrorProto(e))
								.build();
	}
	
	public static String getValue(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static BoolResponse toBoolResponse(boolean value) {
		return BoolResponse.newBuilder()
							.setValue(value)
							.build();
	}
	public static BoolResponse toBoolResponse(Throwable e) {
		return BoolResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static LongResponse toLongResponse(long value) {
		return LongResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static LongResponse toLongResponse(Throwable e) {
		return LongResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static FloatResponse toFloatResponse(float value) {
		return FloatResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static FloatResponse toFloatResponse(Throwable e) {
		return FloatResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(double value) {
		return DoubleResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(Throwable e) {
		return DoubleResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static RecordResponse toRecordResponse(Record value) {
		return RecordResponse.newBuilder()
							.setRecord(PBRecordProtos.toProto(value))
							.build();
	}
	
	public static RecordResponse toRecordResponse(Throwable e) {
		return RecordResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static ErrorProto toErrorProto(Throwable e) {
		Throwable cause = Throwables.unwrapThrowable(e);
		PBMarmotError error = PBMarmotError.fromClass(cause.getClass());
		if ( error != null ) {
			MarmotErrorCode code = MarmotErrorCode.forNumber(error.getCode());
			
			ErrorProto.Builder builder = ErrorProto.newBuilder()
													.setCode(code);
			if ( cause.getMessage() != null ) {
				builder = builder.setDetails(cause.getMessage());
			}
			return builder.build();
		}
		else {
			return ErrorProto.newBuilder()
								.setCode(MarmotErrorCode.ERROR_INTERNAL_ERROR)
								.setDetails(e.toString())
								.build();
		}
	}
	
	public static Exception toException(ErrorProto proto) {
		PBMarmotError error = PBMarmotError.fromCode(proto.getCode().getNumber());
		switch ( proto.getOptionalDetailsCase() ) {
			case DETAILS:
				return error.toException(FOption.of(proto.getDetails()));
			case OPTIONALDETAILS_NOT_SET:
				return error.toException(FOption.empty());
			default:
				throw new AssertionError();
		}
	}
	
	public static <T extends Message> FStream<T> toFStream(Iterator<T> respIter) {
		if ( !respIter.hasNext() ) {
			// Iterator가 empty인 경우는 예외가 발생하지 않았고, 결과가 없는 경우를
			// 의미하기 때문에 empty FStream을 반환한다.
			return FStream.empty();
		}
		
		PeekingIterator<T> piter = Iterators.peekingIterator(respIter);
		T proto = piter.peek();
		FOption<ErrorProto> error = getOptionField(proto, "error");
		if ( error.isPresent() ) {
			throw Throwables.toRuntimeException(toException(error.get()));
		}
		
		return FStream.from(piter);
	}
	
	public static void handle(VoidResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VOID:
				return;
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static String handle(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static List<String> handle(StringListResponse resp) {
		switch ( resp.getEitherCase() ) {
			case LIST:
				return resp.getList().getValueList();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static boolean handle(BoolResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static long handle(LongResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static float handle(FloatResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static double handle(DoubleResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static <X extends Throwable> void replyBoolean(CheckedSupplier<Boolean> supplier,
									StreamObserver<BoolResponse> response) {
		try {
			boolean done = supplier.get();
			response.onNext(BoolResponse.newBuilder()
										.setValue(done)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(BoolResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyLong(CheckedSupplier<Long> supplier,
							StreamObserver<LongResponse> response) {
		try {
			long ret = supplier.get();
			response.onNext(LongResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(LongResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyString(CheckedSupplier<String> supplier,
									StreamObserver<StringResponse> response) {
		try {
			String ret = supplier.get();
			response.onNext(StringResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(StringResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static void replyVoid(CheckedRunnable runnable,
									StreamObserver<VoidResponse> response) {
		try {
			runnable.run();
			response.onNext(VoidResponse.newBuilder()
										.setVoid(VOID)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(VoidResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static byte[] toDelimitedBytes(Message proto) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			proto.writeTo(baos);
		}
		catch ( IOException e ) {
			throw new RuntimeException(e);
		}
		finally {
			IOUtils.closeQuietly(baos);
		}
		
		return baos.toByteArray();
	}
	
	public static String fromProto(StringProto proto) {
		return proto.getValue();
	}
	
	public static StringProto toStringProto(String value) {
		return StringProto.newBuilder().setValue(value).build();
	}
	
	public static boolean fromProto(BoolProto proto) {
		return proto.getValue();
	}
	
	public static BoolProto toProto(boolean value) {
		return BoolProto.newBuilder().setValue(value).build();
	}
	
	public static Size2i fromProto(Size2iProto proto) {
		return new Size2i(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2iProto toProto(Size2i dim) {
		return Size2iProto.newBuilder()
									.setWidth(dim.getWidth())
									.setHeight(dim.getHeight())
									.build();
	}
	
	public static Size2d fromProto(Size2dProto proto) {
		return new Size2d(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2dProto toProto(Size2d dim) {
		return Size2dProto.newBuilder()
						.setWidth(dim.getWidth())
						.setHeight(dim.getHeight())
						.build();
	}
	
	public static Interval fromProto(IntervalProto proto) {
		return Interval.between(proto.getStart(), proto.getEnd());
	}
	
	public static IntervalProto toProto(Interval intvl) {
		return IntervalProto.newBuilder()
							.setStart(intvl.getStartMillis())
							.setEnd(intvl.getEndMillis())
							.build();
	}
	
	public static Coordinate fromProto(CoordinateProto proto) {
		return new Coordinate(proto.getX(), proto.getY());
	}
	
	public static CoordinateProto toProto(Coordinate coord) {
		return CoordinateProto.newBuilder()
								.setX(coord.x)
								.setY(coord.y)
								.build();
	}
	
	public static Point fromProto(PointProto proto) {
		double x = proto.getX();
		if ( Double.isNaN(x) ) {
			return GeoClientUtils.EMPTY_POINT;
		}
		else {
			return GeoClientUtils.toPoint(x, proto.getY());
		}
	}
	
	private static final PointProto EMPTY_POINT
					= PointProto.newBuilder().setX(Double.NaN).setY(Double.NaN).build();
	public static PointProto toProto(Point pt) {
		return pt.isEmpty()
				? EMPTY_POINT
				: PointProto.newBuilder().setX(pt.getX()).setY(pt.getY()).build();
	}
	
	public static Envelope fromProto(EnvelopeProto proto) {
		Coordinate tl = PBUtils.fromProto(proto.getTl());
		if ( !Double.isInfinite(tl.x) ) {
			return new Envelope(tl, fromProto(proto.getBr()));
		}
		else {
			return new Envelope();
		}
	}
	
	public static EnvelopeProto toProto(Envelope envl) {
		if ( !envl.isNull() ) {
			return EnvelopeProto.newBuilder()
								.setTl(CoordinateProto.newBuilder()
													.setX(envl.getMinX())
													.setY(envl.getMinY())
													.build())
								.setBr(CoordinateProto.newBuilder()
													.setX(envl.getMaxX())
													.setY(envl.getMaxY())
													.build())
								.build();
		}
		else {
			return EnvelopeProto.newBuilder()
								.setTl(CoordinateProto.newBuilder()
													.setX(Double.NEGATIVE_INFINITY)
													.setY(Double.NEGATIVE_INFINITY)
													.build())
								.setBr(CoordinateProto.newBuilder()
													.setX(Double.NEGATIVE_INFINITY)
													.setY(Double.NEGATIVE_INFINITY)
													.build())
								.build();
		}
	}
	
	public static Geometry fromProto(GeometryProto proto) {
		switch ( proto.getEitherCase() ) {
			case POINT:
				return fromProto(proto.getPoint());
			case WKB:
				try {
					return GeoClientUtils.fromWKB(proto.getWkb().toByteArray());
				}
				catch ( ParseException e ) {
					throw new IllegalArgumentException("invalid WKB: cause=" + e);
				}
			case NULL:
				return null;
			case EMPTY:
				TypeCode tc = TypeCode.fromCode(proto.getEmpty().getNumber());
				GeometryDataType dt = (GeometryDataType)DataTypes.fromTypeCode(tc);
				return GeoClientUtils.emptyGeometry(dt.toGeometries());
			default:
				throw new AssertionError();
		}
	}
	
	public static TypeCodeProto toProto(TypeCode tc) {
		return TypeCodeProto.forNumber(tc.get());
	}
	public static TypeCode fromProto(TypeCodeProto proto) {
		return TypeCode.fromCode(proto.getNumber());
	}

	private static final GeometryProto NULL_GEOM = GeometryProto.newBuilder().setNull(VOID).build();
	public static GeometryProto toProto(Geometry geom) {
		if ( geom == null ) {
			return NULL_GEOM;
		}
		else if ( geom.isEmpty() ) {
			TypeCode tc = GeometryDataType.fromGeometry(geom).getTypeCode();
			TypeCodeProto tcProto = TypeCodeProto.valueOf(tc.name());
			return GeometryProto.newBuilder().setEmpty(tcProto).build();
		}
		else if ( geom instanceof Point ) {
			Point pt = (Point)geom;
			PointProto ptProto = PointProto.newBuilder()
											.setX(pt.getX())
											.setY(pt.getY())
											.build();
			return GeometryProto.newBuilder().setPoint(ptProto).build();
		}
		else {
			ByteString wkb = ByteString.copyFrom(GeoClientUtils.toWKB(geom));
			return GeometryProto.newBuilder().setWkb(wkb).build();
		}
	}
	
	public static Throwable parsePlanExecutionError(ErrorProto proto) {
		switch ( proto.getCode() ) {
			case ERROR_EXEC_INTERRUPTED:
				return new InterruptedException(proto.getDetails());
			case ERROR_EXEC_CANCELLED:
				return new CancellationException(proto.getDetails());
			case ERROR_EXEC_FAILED:
				return new ExecutionException(new MarmotExecutionException(proto.getDetails()));
			case ERROR_EXEC_TIMED_OUT:
				return new TimeoutException(proto.getDetails());
			default:
				return PBUtils.toException(proto);
		}
	}
	
	public static PropertiesProto toProto(Map<String,String> metadata) {
		List<PropertyProto> properties = KVFStream.from(metadata)
												.map(kv -> PropertyProto.newBuilder()
																		.setKey(kv.key())
																		.setValue(kv.value())
																		.build())
												.toList();
		return PropertiesProto.newBuilder()
							.addAllProperty(properties)
							.build();
	}
	
	public static Map<String,Object> fromProto(KeyValueMapProto kvmProto) {
		return FStream.from(kvmProto.getKeyValueList())
					.toKeyValueStream(proto -> KeyValue.of(proto.getKey(), proto.getValue()))
					.mapValue(vproto -> PBValueProtos.fromProto(vproto))
					.toMap();
	}
	
	public static KeyValueMapProto toKeyValueMapProto(Map<String,Object> keyValueMap) {
		List<KeyValueProto> keyValues
					= KVFStream.from(keyValueMap)
								.map((k,v) -> KeyValueProto.newBuilder()
															.setKey(k)
															.setValue(PBValueProtos.toValueProto(v))
															.build())
								.toList();
		return KeyValueMapProto.newBuilder()
							.addAllKeyValue(keyValues)
							.build();
	}
}
