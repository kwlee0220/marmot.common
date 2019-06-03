package marmot.plan;

import marmot.RecordSchema;
import marmot.proto.optor.ReducerProto;
import marmot.proto.optor.ScriptRecordSetReducerProto;
import marmot.protobuf.PBUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class GroupReducerScriptBuilder {
	private RecordSchema m_intermediateSchema;
	private String m_produceExpr;
	private FOption<String> m_combinerInitializeExpr;
	private String m_combineExpr;
	private RecordSchema m_schema;
	private String m_reduceExpr;
	
	public static GroupReducerScriptBuilder builder() {
		return new GroupReducerScriptBuilder();
	}

	public GroupReducerScriptBuilder intermediateProducer(String intermSchemaStr, String toIntermExpr) {
		m_intermediateSchema = RecordSchema.parse(intermSchemaStr);
		m_produceExpr = toIntermExpr;
		return this;
	}
	
	public GroupReducerScriptBuilder intermediateCombiner(String expr) {
		m_combinerInitializeExpr = FOption.empty();
		m_combineExpr = expr;
		return this;
	}
	public GroupReducerScriptBuilder intermediateCombiner(String initExpr, String expr) {
		m_combinerInitializeExpr = FOption.of(initExpr);
		m_combineExpr = expr;
		return this;
	}
	
	public GroupReducerScriptBuilder reducer(String reducedSchemaStr, String expr) {
		m_schema = RecordSchema.parse(reducedSchemaStr);
		m_reduceExpr = expr;
		return this;
	}
	
	public ReducerProto build() {
		ScriptRecordSetReducerProto.Builder reducerBuilder
					= ScriptRecordSetReducerProto.newBuilder()
											.setOutputSchema(m_schema.toProto())
											.setIntermediateSchema(m_intermediateSchema.toProto())
											.setProducerExpr(m_produceExpr)
											.setCombinerExpr(m_combineExpr)
											.setFinalizerExpr(m_reduceExpr);
		m_combinerInitializeExpr.ifPresent(reducerBuilder::setCombinerInitializeExpr);
		ScriptRecordSetReducerProto proto = reducerBuilder.build();
		
		return ReducerProto.newBuilder()
							.setReducer(PBUtils.serialize(proto))
							.build();
	}
}
