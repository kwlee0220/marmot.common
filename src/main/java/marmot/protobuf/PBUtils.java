package marmot.protobuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.vavr.CheckedRunnable;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import marmot.PlanExecutionException;
import marmot.Record;
import marmot.geo.GeoClientUtils;
import marmot.proto.BoolProto;
import marmot.proto.CoordinateProto;
import marmot.proto.EnvelopeProto;
import marmot.proto.GeometryProto;
import marmot.proto.GridCellProto;
import marmot.proto.IntervalProto;
import marmot.proto.JavaSerializedProto;
import marmot.proto.KeyValueMapProto;
import marmot.proto.KeyValueMapProto.KeyValueProto;
import marmot.proto.MapTileProto;
import marmot.proto.PointProto;
import marmot.proto.PropertiesProto;
import marmot.proto.PropertiesProto.PropertyProto;
import marmot.proto.ProtoBufSerializedProto;
import marmot.proto.SerializedProto;
import marmot.proto.Size2dProto;
import marmot.proto.Size2iProto;
import marmot.proto.StringProto;
import marmot.proto.TypeCodeProto;
import marmot.proto.ValueProto;
import marmot.proto.VoidProto;
import marmot.proto.service.BoolResponse;
import marmot.proto.service.ErrorProto;
import marmot.proto.service.LongResponse;
import marmot.proto.service.MarmotErrorCode;
import marmot.proto.service.RecordResponse;
import marmot.proto.service.ResultProto;
import marmot.proto.service.StringResponse;
import marmot.proto.service.VoidResponse;
import marmot.remote.protobuf.PBMarmotError;
import marmot.support.DateFunctions;
import marmot.support.DateTimeFunctions;
import marmot.support.PBException;
import marmot.support.PBSerializable;
import marmot.support.TimeFunctions;
import marmot.type.DataType;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import marmot.type.GridCell;
import marmot.type.Interval;
import marmot.type.MapTile;
import marmot.type.Trajectory;
import marmot.type.TypeCode;
import utils.Size2d;
import utils.Size2i;
import utils.Throwables;
import utils.Unchecked.CheckedSupplier;
import utils.UnitUtils;
import utils.func.FOption;
import utils.func.Result;
import utils.io.IOUtils;
import utils.stream.FStream;
import utils.stream.KVFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBUtils {
	public static final VoidProto VOID = VoidProto.newBuilder().build();
	private static final VoidResponse VOID_RESPONSE = VoidResponse.newBuilder()
																	.setVoid(VOID)
																	.build();

	public static boolean isUnavailableException(Throwable cause) {
		if ( cause instanceof StatusRuntimeException ) {
			Status status = ((StatusRuntimeException)cause).getStatus();
			if ( status.getCode() == Status.CANCELLED.getCode() ) {
				return true;
			}
		}
		
		return false;
	}
	
	public static final SerializedProto serializeObject(Object obj) {
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
										.setSerialized(ByteString.copyFrom(IOUtils.serialize(obj)))
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
			return (T)IOUtils.deserialize(proto.getSerialized().toByteArray());
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
			return FOption.ofNullable((T)KVFStream.of(proto.getAllFields())
												.filter(kv -> kv.key().getName().equals(field))
												.next()
												.map(kv -> kv.value())
												.getOrNull());
					}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}

	public static FOption<String> getOptionalStringField(Message proto, String field) {
		try {
			return KVFStream.of(proto.getAllFields())
								.filter(kv -> kv.key().getName().equals(field))
								.next()
								.map(kv -> (String)kv.value());
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T getField(Message proto, String field) {
		try {
			return(T)KVFStream.of(proto.getAllFields())
									.filter(kv -> kv.key().getName().equals(field))
									.next()
									.map(kv -> kv.value())
									.getOrElseThrow(()
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
	
	public static RecordResponse toRecordResponse(Record value) {
		return RecordResponse.newBuilder()
							.setRecord(value.toProto())
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
				return error.toException(Option.some(proto.getDetails()));
			case OPTIONALDETAILS_NOT_SET:
				return error.toException(Option.none());
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
	
	public static void reply(CheckedSupplier<Boolean> supplier,
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
	
	public static void replyLong(CheckedSupplier<Long> supplier,
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
	
	public static void replyString(CheckedSupplier<String> supplier,
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
		CoordinateProto tl = proto.getTl();
		CoordinateProto br = proto.getBr();
		return new Envelope(new Coordinate(tl.getX(), tl.getY()),
							new Coordinate(br.getX(), br.getY()));
	}
	
	public static EnvelopeProto toProto(Envelope envl) {
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

	private static final GeometryProto NULL_GEOM = GeometryProto.newBuilder().setNull(VOID).build();
	public static GeometryProto toProto(Geometry geom) {
		if ( geom == null ) {
			return NULL_GEOM;
		}
		else if ( geom.isEmpty() ) {
			TypeCode tc = GeometryDataType.fromGeometries(geom).getTypeCode();
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
			case ERROR_PLAN_EXECUTION_INTERRUPTED:
				return new InterruptedException(proto.getDetails());
			case ERROR_PLAN_EXECUTION_CANCELLED:
				return new CancellationException(proto.getDetails());
			case ERROR_PLAN_EXECUTION_FAILED:
				return new ExecutionException(new PlanExecutionException(proto.getDetails()));
			case ERROR_PLAN_EXECUTION_TIMED_OUT:
				return new TimeoutException(proto.getDetails());
			default:
				return PBUtils.toException(proto);
		}
	}
	
	public static Result<Void> fromProto(ResultProto proto) {
		switch ( proto.getEitherCase() ) {
			case VALUE:
				return Result.some(null);
			case FAILURE:
				return Result.failure(parsePlanExecutionError(proto.getFailure()));
			case NONE:
				return  Result.none();
			default:
				throw new AssertionError();
		}
	}
	
	public static ResultProto toProto(Result<Void> result) {
		ResultProto.Builder builder = ResultProto.newBuilder();
		if ( result.isSuccess() ) {
			builder.setValue(PBUtils.toValueProto(null));
		}
		else if ( result.isFailure() ) {
			builder.setFailure(PBUtils.toErrorProto(result.getCause()));
		}
		else {
			builder.setNone(PBUtils.VOID);
		}
		return builder.build();
	}
	
	public static Map<String,String> fromProto(PropertiesProto proto) {
		return FStream.from(proto.getPropertyList())
				.foldLeft(Maps.newHashMap(), (map,kv) -> {
					map.put(kv.getKey(), kv.getValue());
					return map;
				});
	}
	
	public static PropertiesProto toProto(Map<String,String> metadata) {
		List<PropertyProto> properties = KVFStream.of(metadata)
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
					.toKVFStream(KeyValueProto::getKey, KeyValueProto::getValue)
					.mapValue(vproto -> fromProto(vproto)._2)
					.toMap();
	}
	
	public static KeyValueMapProto toKeyValueMapProto(Map<String,Object> keyValueMap) {
		List<KeyValueProto> keyValues = KVFStream.of(keyValueMap)
												.map(kv -> KeyValueProto.newBuilder()
																	.setKey(kv.key())
																	.setValue(toValueProto(kv.value()))
																	.build())
												.toList();
		return KeyValueMapProto.newBuilder()
							.addAllKeyValue(keyValues)
							.build();
	}
	
	private static final int TOO_BIG = (int)UnitUtils.parseByteSize("4mb");
	private static final int STRING_COMPRESS_THRESHOLD = (int)UnitUtils.parseByteSize("512kb");
	private static final int BINARY_COMPRESS_THRESHOLD = (int)UnitUtils.parseByteSize("1mb");
	public static ValueProto toValueProto(TypeCode tc, Object obj) {
		if ( obj == null ) {
			return ValueProto.newBuilder()
							.setNullValue(TypeCodeProto.valueOf(tc.name()))
							.build();
		}
		
		ValueProto.Builder builder = ValueProto.newBuilder();
		switch ( tc ) {
			case BYTE:
				builder.setByteValue((byte)obj);
				break;
			case SHORT:
				builder.setShortValue((short)obj);
				break;
			case INT:
				builder.setIntValue((int)obj);
				break;
			case LONG:
				builder.setLongValue((long)obj);
				break;
			case FLOAT:
				builder.setFloatValue((float)obj);
				break;
			case DOUBLE:
				builder.setDoubleValue((double)obj);
				break;
			case BOOLEAN:
				builder.setBoolValue((boolean)obj);
				break;
			case STRING:
				String str = ((String)obj);
				if ( str.length() < STRING_COMPRESS_THRESHOLD ) {
					builder.setStringValue(str);
				}
				else {
					try {
						byte[] compressed = IOUtils.compress(str.getBytes());
						if ( compressed.length > TOO_BIG ) {
							throw new PBException("string value is too big: size="
												+ UnitUtils.toByteSizeString(compressed.length));
						}
						builder.setCompressedStringValue(ByteString.copyFrom(compressed));
					}
					catch ( IOException e ) {
						throw new PBException(e);
					}
				}
				break;
			case BINARY:
				byte[] bytes = (byte[])obj;
				if ( bytes.length < BINARY_COMPRESS_THRESHOLD ) {
					builder.setBinaryValue(ByteString.copyFrom(bytes));
				}
				else {
					try {
						byte[] compressed = IOUtils.compress(bytes);
						if ( compressed.length > TOO_BIG ) {
							throw new PBException("binary value is too big: size="
												+ UnitUtils.toByteSizeString(compressed.length));
						}
						builder.setCompressedBinaryValue(ByteString.copyFrom(compressed));
					}
					catch ( IOException e ) {
						throw new PBException(e);
					}
				}
				break;
			case DATETIME:
				builder.setDatetimeValue(DateTimeFunctions.ST_DTToMillis(obj));
				break;
			case DATE:
				builder.setDateValue(DateFunctions.ST_DateToMillis(obj));
				break;
			case TIME:
				builder.setTimeValue(TimeFunctions.ST_TimeToString(obj));
				break;
			case INTERVAL:
				builder.setIntervalValue(toProto((Interval)obj));
				break;
			case ENVELOPE:
				builder.setEnvelopeValue(PBUtils.toProto((Envelope)obj));
				break;
			case TILE:
				MapTile tile = (MapTile)obj;
				builder.setTileValue(MapTileProto.newBuilder()
												.setX(tile.getX())
												.setY(tile.getY())
												.setZoom(tile.getZoom())
												.build());
				break;
			case GRID_CELL:
				GridCell cell = (GridCell)obj;
				builder.setGridCellValue(GridCellProto.newBuilder()
														.setX(cell.getX())
														.setY(cell.getY())
														.build());
				break;
			case POINT:
				builder.setPointValue(PBUtils.toProto((Point)obj));
				break;
			case MULTI_POINT:
			case LINESTRING:
			case MULTI_LINESTRING:
			case POLYGON:
			case MULTI_POLYGON:
			case GEOM_COLLECTION:
			case GEOMETRY:
				builder.setGeometryValue(PBUtils.toProto((Geometry)obj));
				break;
			case TRAJECTORY:
				builder.setTrajectoryValue(((Trajectory)obj).toProto());
				break;
			default:
				throw new AssertionError();
		}
		
		return builder.build();
	}
	
	public static Tuple2<DataType,Object> fromProto(ValueProto proto) {
		switch ( proto.getValueCase() ) {
			case BYTE_VALUE:
				return Tuple.of(DataType.BYTE, (byte)proto.getByteValue());
			case SHORT_VALUE:
				return Tuple.of(DataType.SHORT, (short)proto.getShortValue());
			case INT_VALUE:
				return Tuple.of(DataType.INT, (int)proto.getIntValue());
			case LONG_VALUE:
				return Tuple.of(DataType.LONG, proto.getLongValue());
			case FLOAT_VALUE:
				return Tuple.of(DataType.FLOAT, proto.getFloatValue());
			case DOUBLE_VALUE:
				return Tuple.of(DataType.DOUBLE, proto.getDoubleValue());
			case BOOL_VALUE:
				return Tuple.of(DataType.BOOLEAN, proto.getBoolValue());
			case STRING_VALUE:
				return Tuple.of(DataType.STRING, proto.getStringValue());
			case COMPRESSED_STRING_VALUE:
				try {
					byte[] bytes = proto.getCompressedStringValue().toByteArray();
					bytes = IOUtils.decompress(bytes);
					return Tuple.of(DataType.STRING, new String(bytes));
				}
				catch ( Exception e ) {
					throw new PBException(e);
				}
			case BINARY_VALUE:
				return Tuple.of(DataType.BINARY, proto.getBinaryValue().toByteArray());
			case COMPRESSED_BINARY_VALUE:
				try {
					byte[] bytes = proto.getCompressedBinaryValue().toByteArray();
					return Tuple.of(DataType.BINARY, IOUtils.decompress(bytes));
				}
				catch ( Exception e ) {
					throw new PBException(e);
				}
			case DATETIME_VALUE:
				return Tuple.of(DataType.DATETIME, DateTimeFunctions.ST_DTFromMillis(proto.getDatetimeValue()));
			case DATE_VALUE:
				return Tuple.of(DataType.DATE, DateFunctions.ST_DateFromMillis(proto.getDateValue()));
			case TIME_VALUE:
				return Tuple.of(DataType.TIME, TimeFunctions.ST_TimeFromString(proto.getTimeValue()));
			case DURATION_VALUE:
				throw new UnsupportedOperationException("duration type");
			case INTERVAL_VALUE:
				IntervalProto intvlProto = proto.getIntervalValue();
				return Tuple.of(DataType.INTERVAL,
							Interval.between(intvlProto.getStart(), intvlProto.getEnd()));
			case ENVELOPE_VALUE:
				return Tuple.of(DataType.ENVELOPE, PBUtils.fromProto(proto.getEnvelopeValue()));
			case TILE_VALUE:
				MapTileProto mtp = proto.getTileValue();
				return Tuple.of(DataType.TILE, new MapTile(mtp.getZoom(), mtp.getX(), mtp.getY()));
			case GRID_CELL_VALUE:
				GridCellProto gcProto = proto.getGridCellValue();
				return Tuple.of(DataType.GRID_CELL, new GridCell(gcProto.getX(), gcProto.getY()));
			case POINT_VALUE:
				return Tuple.of(DataType.POINT, PBUtils.fromProto(proto.getPointValue()));
			case GEOMETRY_VALUE:
				Geometry geom = PBUtils.fromProto(proto.getGeometryValue());
				DataType type = GeometryDataType.fromGeometries(geom);
				return Tuple.of(type, geom);
			case TRAJECTORY_VALUE:
				Trajectory trj = Trajectory.fromProto(proto.getTrajectoryValue());
				return Tuple.of(DataType.TRAJECTORY, trj);
			case NULL_VALUE:
				TypeCode tc = TypeCode.valueOf(proto.getNullValue().name());
				return Tuple.of(DataTypes.fromTypeCode(tc), null);
			case VALUE_NOT_SET:
				return Tuple.of(null, null);
			default:
				throw new AssertionError();
		}
	}
	
	public static ValueProto toValueProto(Object obj) {
		if ( obj == null ) {
			return ValueProto.newBuilder().build();
		}
		
		ValueProto.Builder builder = ValueProto.newBuilder();
		if ( obj instanceof String ) {
			builder.setStringValue((String)obj);
		}
		else if ( obj instanceof Integer ) {
			builder.setIntValue((int)obj);
		}
		else if ( obj instanceof Double ) {
			builder.setDoubleValue((double)obj);
		}
		else if ( obj instanceof Long ) {
			builder.setLongValue((long)obj);
		}
		else if ( obj instanceof Boolean ) {
			builder.setBoolValue((boolean)obj);
		}
		else if ( obj instanceof Point ) {
			builder.setPointValue(PBUtils.toProto((Point)obj));
		}
		else if ( obj instanceof Geometry ) {
			builder.setGeometryValue(PBUtils.toProto((Geometry)obj));
		}
		else if ( obj instanceof Envelope ) {
			builder.setEnvelopeValue(PBUtils.toProto((Envelope)obj));
		}
		else if ( obj instanceof Byte[] ) {
			builder.setBinaryValue(ByteString.copyFrom((byte[])obj));
		}
		else if ( obj instanceof Byte ) {
			builder.setByteValue((byte)obj);
		}
		else if ( obj instanceof Short ) {
			builder.setShortValue((short)obj);
		}
		else if ( obj instanceof Float ) {
			builder.setFloatValue((float)obj);
		}
		else if ( obj instanceof LocalDateTime ) {
			builder.setDatetimeValue(DateTimeFunctions.ST_DTToMillis(obj));
		}
		else if ( obj instanceof LocalDate ) {
			builder.setDateValue(DateFunctions.ST_DateToMillis(obj));
		}
		else if ( obj instanceof LocalTime ) {
			builder.setTimeValue(TimeFunctions.ST_TimeToString(obj));
		}
		else if ( obj instanceof MapTile ) {
			MapTile tile = (MapTile)obj;
			builder.setTileValue(MapTileProto.newBuilder()
											.setX(tile.getX())
											.setY(tile.getY())
											.setZoom(tile.getZoom())
											.build());
		}
		else if ( obj instanceof GridCell ) {
			GridCell cell = (GridCell)obj;
			builder.setGridCellValue(GridCellProto.newBuilder()
													.setX(cell.getX())
													.setY(cell.getY())
													.build());
		}
		else if ( obj instanceof Trajectory ) {
			builder.setTrajectoryValue(((Trajectory)obj).toProto());
		}
		else {
			throw new AssertionError();
		}
		
		return builder.build();
	}
}
