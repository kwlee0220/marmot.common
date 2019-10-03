package marmot.exec;

import java.util.List;

import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisProto.MemberCase;
import marmot.proto.service.MarmotAnalysisProto.SystemExecProto;
import utils.Utilities;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SystemAnalysis extends MarmotAnalysis {
	private final String m_funcId;
	private final List<String> m_args;
	
	public SystemAnalysis(String id, String funcId, List<String> args) {
		super(id, Type.SYSTEM);
		
		m_funcId = funcId;
		m_args = args;
	}
	
	public String getFunctionId() {
		return m_funcId;
	}
	
	public List<String> getArguments() {
		return m_args;
	}
	
	@Override
	public String toString() {
		String argStr = FStream.from(m_args).join(" ");
		return String.format("%s: %s", m_funcId, argStr);
	}
	
	public static SystemAnalysis fromProto(MarmotAnalysisProto proto) {
		Utilities.checkArgument(proto.getMemberCase().equals(MemberCase.SYSTEM_EXEC),
								"not SystemAnalysis");
		
		SystemExecProto sysFunc = proto.getSystemExec();
		List<String> args = sysFunc.getFunctionArgList();
		
		return new SystemAnalysis(proto.getId(), sysFunc.getFunctionId(), args);
	}

	@Override
	public MarmotAnalysisProto toProto() {
		SystemExecProto sysFunc = SystemExecProto.newBuilder()
										.setFunctionId(m_funcId)
										.addAllFunctionArg(m_args)
										.build();
		return MarmotAnalysisProto.newBuilder()
									.setId(getId())
									.setSystemExec(sysFunc)
									.build();
	}
}
