package marmot.protobuf;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.proto.RecordProto;
import marmot.proto.ValueProto;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBRecordProtos {
	private PBRecordProtos() {
		throw new AssertionError("Should not be called: " + getClass());
	}
	
	public static RecordProto toProto(Record record) {
		return toProto(record.getRecordSchema(), record.getAll());
	}

	public static RecordProto toProto(RecordSchema schema, Object[] values) {
		RecordProto.Builder builder = RecordProto.newBuilder();
		
		for ( int i =0; i < values.length; ++i ) {
			Column col = schema.getColumnAt(i);
			
			ValueProto vproto = PBValueProtos.toValueProto(col.type().getTypeCode(), values[i]);
			builder.addColumn(vproto);
		}
		
		return builder.build();
	}
	
	public static RecordProto toProto(Object[] values) {
		return FStream.of(values)
						.map(PBValueProtos::toValueProto)
						.foldLeft(RecordProto.newBuilder(), (b,p) -> b.addColumn(p))
						.build();
	}
	
	public static void fromProto(RecordProto proto, RecordSchema schema, Object[] values) {
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			values[i] = PBValueProtos.fromProto(proto.getColumn(i));
		}
	}
	
	public static void fromProto(RecordProto proto, Record record) {
		for ( int i =0; i < record.getColumnCount(); ++i ) {
			record.set(i, PBValueProtos.fromProto(proto.getColumn(i)));
		}
	}
}
