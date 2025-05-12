package marmot.exec;

import java.util.Map;

import utils.KeyValue;
import utils.Utilities;
import utils.stream.KeyValueFStream;

import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisProto.MemberCase;
import marmot.proto.service.MarmotAnalysisProto.ModuleExecProto;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ModuleAnalysis extends MarmotAnalysis {
	private final String m_moduleId;
	private final Map<String,String> m_args;
	
	public ModuleAnalysis(String id, String moduleId, Map<String,String> args) {
		super(id, Type.MODULE);

		m_moduleId = moduleId;
		m_args = args;
	}
	
	public String getModuleId() {
		return m_moduleId;
	}
	
	public Map<String,String> getArguments() {
		return m_args;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new  StringBuilder();
		builder.append(String.format("%s[%s]: %s%n", getType(), getId(), getModuleId()));
		for ( Map.Entry<String, String> ent: m_args.entrySet() ) {
			builder.append(String.format("   %10s: %s%n", ent.getKey(), ent.getValue()));
		}
		
		return builder.toString();
	}
	
	public static ModuleAnalysis fromProto(MarmotAnalysisProto proto) {
		Utilities.checkArgument(proto.getMemberCase().equals(MemberCase.MODULE_EXEC), "not ModuleAnalysis");

		
		ModuleExecProto module = proto.getModuleExec();
		String argsStr = module.getModuleArgs();
		Map<String,String> args = Utilities.parseKeyValueMap(argsStr, ';');
		
		return new ModuleAnalysis(proto.getId(), module.getModuleId(), args);
	}

	@Override
	public MarmotAnalysisProto toProto() {
		String argsExpr = KeyValueFStream.from(m_args)
										.map(KeyValue::toString)
										.join("; ");
		
		ModuleExecProto module = ModuleExecProto.newBuilder()
												.setModuleId(m_moduleId)
												.setModuleArgs(argsExpr)
												.build();
		return MarmotAnalysisProto.newBuilder()
									.setId(getId())
									.setModuleExec(module)
									.build();
	}
}
