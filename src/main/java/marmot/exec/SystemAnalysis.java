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
	
	public static SystemAnalysis deleteDataSet(String id, Iterable<String> dsIds) {
		return new SystemAnalysis(id, "delete_dataset", dsIds);
	}
	
	public static SystemAnalysis deleteDir(String id, String... dirs) {
		return new SystemAnalysis(id, "delete_dir", dirs);
	}
	
	public static SystemAnalysis deleteDataSet(String id, String... dsIds) {
		List<String> args = FStream.of(dsIds).toList();
		
		return new SystemAnalysis(id, "delete_dataset", args);
	}
	
	public static SystemAnalysis moveDataSet(String id, String srcDsId, String tarDsId) {
		List<String> args = FStream.of(srcDsId, tarDsId).toList();
		
		return new SystemAnalysis(id, "move_dataset", args);
	}
	
	public static SystemAnalysis createThumbnail(String id, String dsId, int sampleCount) {
		List<String> args = FStream.of(dsId).toList();
		args.add("" + sampleCount);
		
		return new SystemAnalysis(id, "create_thumbnail", args);
	}
	
	public SystemAnalysis(String id, String funcId, Iterable<String> args) {
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
		String argStr = FStream.from(m_args).join(", ");
		return String.format("%s[%s]: %s(%s)", getType(), getId(), getFunctionId(), argStr);
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
