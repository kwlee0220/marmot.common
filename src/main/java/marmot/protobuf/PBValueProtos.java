package marmot.protobuf;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

import marmot.proto.DoubleArrayProto;
import marmot.proto.FloatArrayProto;
import marmot.proto.GridCellProto;
import marmot.proto.IntervalProto;
import marmot.proto.MapTileProto;
import marmot.proto.PropertiesProto;
import marmot.proto.TypeCodeProto;
import marmot.proto.ValueArrayProto;
import marmot.proto.ValueProto;
import marmot.type.DataType;
import marmot.type.DataTypes;
import marmot.type.GeometryDataType;
import marmot.type.GridCell;
import marmot.type.Interval;
import marmot.type.MapTile;
import marmot.type.Trajectory;
import marmot.type.TypeCode;
import utils.LocalDateTimes;
import utils.LocalTimes;
import utils.Utilities;
import utils.func.Tuple;
import utils.stream.DoubleFStream;
import utils.stream.FStream;
import utils.stream.FloatFStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBValueProtos {
	private PBValueProtos() {
		throw new AssertionError("Should not be called: " + getClass());
	}
	
	//
	// TypeCode 정보가 있는 경우의 Object transform
	//
	public static ValueProto toValueProto(TypeCode tc, Object obj) {
		ValueProto.Builder builder = ValueProto.newBuilder();
		
		if ( obj == null ) {
			return builder.setNullValue(TypeCodeProto.valueOf(tc.name())).build();
		}
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
				builder.setStringValue((String)obj);
				break;
			case BINARY:
				builder.setBinaryValue(ByteString.copyFrom((byte[])obj));
				break;
			case DATETIME:
				builder.setDatetimeValue(LocalDateTimes.toUtcMillis((LocalDateTime)obj));
				break;
			case DATE:
				builder.setDateValue(((Date)obj).getTime());
				break;
			case TIME:
				builder.setTimeValue(LocalTimes.toString((LocalTime)obj));
				break;
			case INTERVAL:
				builder.setIntervalValue(PBUtils.toProto((Interval)obj));
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
			case DOUBLE_ARRAY:
				DoubleArrayProto darray = DoubleFStream.of((Double[])obj)
									.foldLeft(DoubleArrayProto.newBuilder(), (b,v) -> b.addElement(v))
									.build();
				builder.setDoubleArray(darray);
				break;
			case FLOAT_ARRAY:
				FloatArrayProto floatArray = FloatFStream.of((Float[])obj)
									.foldLeft(FloatArrayProto.newBuilder(), (b,v) -> b.addElement(v))
									.build();
				builder.setFloatArray(floatArray);
				break;
			default:
				throw new AssertionError();
		}
		
		return builder.build();
	}

	//
	// TypeCode 정보가 없는 경우의 Object transform
	//
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
			builder.setDatetimeValue(Utilities.toUTCEpocMillis((LocalDateTime)obj));
		}
		else if ( obj instanceof Date ) {
			builder.setDateValue(((Date)obj).getTime());
		}
		else if ( obj instanceof LocalTime ) {
			builder.setTimeValue(((LocalTime)obj).toString());
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
	
	//
	//
	//

	public static Tuple<DataType,Object> fromProto2(ValueProto proto) {
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
			case BINARY_VALUE:
				return Tuple.of(DataType.BINARY, proto.getBinaryValue().toByteArray());
			case DATETIME_VALUE:
				return Tuple.of(DataType.DATETIME, LocalDateTimes.fromUtcMillis(proto.getDatetimeValue()));
			case DATE_VALUE:
				return Tuple.of(DataType.DATE, new Date(proto.getDateValue()));
			case TIME_VALUE:
				return Tuple.of(DataType.TIME, LocalTimes.fromString(proto.getTimeValue()));
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
				DataType type = GeometryDataType.fromGeometry(geom);
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

	public static Object fromProto(ValueProto proto) {
		switch ( proto.getValueCase() ) {
			case BYTE_VALUE:
				return (byte)proto.getByteValue();
			case SHORT_VALUE:
				return (short)proto.getShortValue();
			case INT_VALUE:
				return proto.getIntValue();
			case LONG_VALUE:
				return proto.getLongValue();
			case FLOAT_VALUE:
				return proto.getFloatValue();
			case DOUBLE_VALUE:
				return proto.getDoubleValue();
			case BOOL_VALUE:
				return proto.getBoolValue();
			case STRING_VALUE:
				return proto.getStringValue();
			case BINARY_VALUE:
				return proto.getBinaryValue().toByteArray();
			case DATETIME_VALUE:
				return LocalDateTimes.fromUtcMillis(proto.getDatetimeValue());
			case DATE_VALUE:
				return new Date(proto.getDateValue());
			case TIME_VALUE:
				return LocalTimes.fromString(proto.getTimeValue());
			case DURATION_VALUE:
				throw new UnsupportedOperationException("duration type");
			case INTERVAL_VALUE:
				IntervalProto intvlProto = proto.getIntervalValue();
				return Interval.between(intvlProto.getStart(), intvlProto.getEnd());
			case ENVELOPE_VALUE:
				return PBUtils.fromProto(proto.getEnvelopeValue());
			case TILE_VALUE:
				MapTileProto mtp = proto.getTileValue();
				return Tuple.of(DataType.TILE, new MapTile(mtp.getZoom(), mtp.getX(), mtp.getY()));
			case GRID_CELL_VALUE:
				GridCellProto gcProto = proto.getGridCellValue();
				return new GridCell(gcProto.getX(), gcProto.getY());
			case POINT_VALUE:
				return PBUtils.fromProto(proto.getPointValue());
			case GEOMETRY_VALUE:
				return PBUtils.fromProto(proto.getGeometryValue());
			case TRAJECTORY_VALUE:
				return Trajectory.fromProto(proto.getTrajectoryValue());
			case DOUBLE_ARRAY:
				return DoubleFStream.from(proto.getDoubleArray().getElementList()).toArray();
			case FLOAT_ARRAY:
				return FloatFStream.from(proto.getFloatArray().getElementList()).toArray();
			case NULL_VALUE:
			case VALUE_NOT_SET:
				return null;
			default:
				throw new AssertionError();
		}
	}

	public static ValueArrayProto toValueArrayProto(Object[] values) {
		return ValueArrayProto.newBuilder()
								.addAllValue(FStream.of(values)
													.map(PBValueProtos::toValueProto)
													.toList())
								.build();
	}

	public static ValueArrayProto toValueArrayProto(List<Object> values) {
		return ValueArrayProto.newBuilder()
								.addAllValue(FStream.from(values)
													.map(PBValueProtos::toValueProto)
													.toList())
								.build();
	}

	public static List<Object> fromProto(ValueArrayProto proto) {
		return FStream.from(proto.getValueList())
						.map(PBValueProtos::fromProto)
						.toList();
	}

	public static Map<String,String> fromProto(PropertiesProto proto) {
		return FStream.from(proto.getPropertyList())
				.foldLeft(Maps.newHashMap(), (map,kv) -> {
					map.put(kv.getKey(), kv.getValue());
					return map;
				});
	}
}
