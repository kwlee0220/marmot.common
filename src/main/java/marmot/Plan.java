package marmot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.vavr.control.Option;
import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.PlanProto;
import marmot.support.PBSerializable;
import utils.io.IOUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Plan implements PBSerializable<PlanProto> {
	private final PlanProto m_proto;
	
	public int length() {
		return m_proto.getOperatorsCount();
	}
	
	public Plan setName(String name) {
		return new Plan(m_proto.toBuilder()
								.setName(name)
								.build());
	}
	
	public List<OperatorProto> getOperatorProtos() {
		return m_proto.getOperatorsList();
	}
	
	public Option<OperatorProto> getLastOperator() {
		if ( length() == 0 ) {
			return Option.none();
		}
		else {
			return Option.some(m_proto.getOperators(m_proto.getOperatorsCount()-1));
		}
	}
	
	public static final Plan fromBytes(byte[] bytes) {
		try {
			return new Plan(PlanProto.parseFrom(bytes));
		}
		catch ( InvalidProtocolBufferException e ) {
			throw new PlanExecutionException(e);
		}
	}
	
	public static Plan fromEncodedString(String encoded) {
		return fromBytes(IOUtils.destringify(encoded));
	}
	
	public static final Plan fromFile(File file) {
		try ( FileInputStream fis = new FileInputStream(file) ) {
			return Plan.fromProto(PlanProto.parseFrom(fis));
		}
		catch ( IOException e ) {
			throw new PlanExecutionException(e);
		}
	}
	
	public static Plan fromProto(PlanProto proto) {
		return new Plan(proto);
	}
	
	public static Plan parseJson(Reader input) throws IOException {
		try ( Reader reader = input ) {
			PlanProto.Builder builder = PlanProto.newBuilder();
			JsonFormat.parser().merge(reader, builder);
			return Plan.fromProto(builder.build());
		}
	}
	
	public static Plan parseJson(String json) throws InvalidProtocolBufferException {
		PlanProto.Builder builder = PlanProto.newBuilder();
		JsonFormat.parser().merge(json, builder);
		return Plan.fromProto(builder.build());
	}
	
	private Plan(PlanProto proto) {
		m_proto = proto;
	}
	
	public String getName() {
		return m_proto.getName();
	}
	
	public static PlanBuilder builder(String name) {
		return new PlanBuilder(name);
	}
	
	public static PlanBuilder builder() {
		return new PlanBuilder("plan");
	}
	
	public String toJson() throws InvalidProtocolBufferException {
		return JsonFormat.printer().print(m_proto);
	}
	
	public PlanBuilder toBuilder() {
		return FStream.of(m_proto.getOperatorsList())
					.foldLeft(new PlanBuilder(getName()), (b,o) -> b.add(o));
	}
	
	public void save(File file) {
		try ( FileOutputStream fos = new FileOutputStream(file) ) {
			m_proto.writeTo(fos);
		}
		catch ( IOException e ) {
			throw new PlanExecutionException(e);
		}
	}
	
	public String toEncodedString() {
		return IOUtils.stringify(m_proto.toByteArray());
	}
	
	@Override
	public String toString() {
		return m_proto.toString();
	}

	@Override
	public PlanProto toProto() {
		return m_proto;
	}
	
	public static Plan concat(Plan plan1, Plan plan2) {
		PlanBuilder builder = new PlanBuilder(plan1.getName());
		
		FStream.of(plan1.m_proto.getOperatorsList())
				.foldLeft(builder, (b,o) -> b.add(o));
		FStream.of(plan2.m_proto.getOperatorsList())
				.foldLeft(builder, (b,o) -> b.add(o));
		
		return builder.build();
	}
}
