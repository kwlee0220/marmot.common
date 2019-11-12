package marmot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;

import marmot.proto.optor.OperatorProto;
import marmot.proto.optor.PlanProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.func.FOption;
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
	
	public FOption<OperatorProto> getLastOperator() {
		if ( length() == 0 ) {
			return FOption.empty();
		}
		else {
			return FOption.of(m_proto.getOperators(m_proto.getOperatorsCount()-1));
		}
	}
	
	public static final Plan fromBytes(byte[] bytes) throws IOException {
		try {
			return new Plan(PlanProto.parseFrom(bytes));
		}
		catch ( InvalidProtocolBufferException e ) {
			throw new IOException(e);
		}
	}
	
	public static Plan fromEncodedString(String encoded) throws IOException {
		return fromBytes(IOUtils.destringify(encoded));
	}
	
	public static final Plan fromFile(File file) throws FileNotFoundException, IOException {
		try ( FileInputStream fis = new FileInputStream(file) ) {
			return Plan.fromProto(PlanProto.parseFrom(fis));
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
	
	public static Plan parseJson(String json) throws IOException {
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
	
	public String toJson(boolean omitWhiteSpace) throws IOException {
		return PBUtils.toJson(m_proto, omitWhiteSpace);
	}
	
	public PlanBuilder toBuilder() {
		return FStream.from(m_proto.getOperatorsList())
					.foldLeft(new PlanBuilder(getName()), (b,o) -> b.add(o));
	}
	
	public void save(File file) throws IOException {
		try ( FileOutputStream fos = new FileOutputStream(file) ) {
			m_proto.writeTo(fos);
		}
	}
	
	public String toEncodedString() {
		return IOUtils.stringify(m_proto.toByteArray());
	}
	
	@Override
	public String toString() {
		return TextFormat.printToUnicodeString(m_proto);
	}

	@Override
	public PlanProto toProto() {
		return m_proto;
	}
	
	public static Plan concat(Plan plan1, Plan plan2) {
		PlanBuilder builder = new PlanBuilder(plan1.getName());
		
		FStream.from(plan1.m_proto.getOperatorsList())
				.foldLeft(builder, (b,o) -> b.add(o));
		FStream.from(plan2.m_proto.getOperatorsList())
				.foldLeft(builder, (b,o) -> b.add(o));
		
		return builder.build();
	}
}
