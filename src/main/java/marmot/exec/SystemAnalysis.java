package marmot.exec;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import marmot.geo.command.ClusterDataSetOptions;
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
	
	public static SystemAnalysis clusterDataSet(String id, String dsId, ClusterDataSetOptions opts) {
		List<String> args = new ArrayList<>();
		args.add(dsId);
		opts.workerCount().ifPresent(c -> args.add(String.format("workers=%d", c)));
		
		return new SystemAnalysis(id, "cluster_dataset", args);
	}
	
	public static SystemAnalysis clusterDataSet(String id, String dsId) {
		return clusterDataSet(id, dsId, ClusterDataSetOptions.DEFAULT());
	}
	
	public static SystemAnalysis deleteDataSet(String id, String... dsIds) {
		List<String> args = FStream.of(dsIds).toList();
		
		return new SystemAnalysis(id, "delete_dataset", args);
	}
	
	public SystemAnalysis(String id, String funcId, List<String> args) {
		super(id, Type.SYSTEM);
		
		m_funcId = funcId;
		m_args = Lists.newArrayList(args);
	}
	
	public SystemAnalysis(String id, String funcId, String... args) {
		super(id, Type.SYSTEM);
		
		m_funcId = funcId;
		m_args = Lists.newArrayList(args);
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
		return String.format("%s[%s]: %s(%s)", getType(), getId(), getFunctionId(), getArguments());
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
