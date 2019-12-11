package marmot.analysis.system;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

import marmot.exec.MarmotAnalysis;
import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisProto.MemberCase;
import marmot.proto.service.MarmotAnalysisProto.SystemExecProto;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SystemAnalysis extends MarmotAnalysis {
	private final String m_funcId;
	private final List<String> m_args;
	
	public static Set<String> getSystemAnalysisClassIdAll() {
		return FStream.of(SystemAnalysis.class.getMethods())
					.flatMapOption(m -> FOption.ofNullable(m.getAnnotation(SystemAnalysisDef.class)))
					.map(SystemAnalysisDef::id)
					.toSet();
	}
	
	public static List<String> getSystemParameterNameAll(String id) {
		return FStream.of(SystemAnalysis.class.getMethods())
					.flatMapOption(m -> FOption.ofNullable(m.getAnnotation(SystemAnalysisDef.class)))
					.filter(def -> def.id().equals(id))
					.flatMapArray(def -> def.parameters())
					.toList();
	}

	@SystemAnalysisDef(id="cluster_dataset", parameters= {"dataset_id"})
	public static SystemAnalysis clusterDataSet(String id, String dsId) {
		return new SystemAnalysis(id, "cluster_dataset", Lists.newArrayList(dsId));
	}

	@SystemAnalysisDef(id="delete_dataset", parameters= {"dataset_id"})
	public static SystemAnalysis deleteDataSet(String id, String dsId) {
		return new SystemAnalysis(id, "delete_dataset", dsId);
	}

	@SystemAnalysisDef(id="delete_dir", parameters= {"directory"})
	public static SystemAnalysis deleteDir(String id, String dir) {
		return new SystemAnalysis(id, "delete_dir", dir);
	}

	@SystemAnalysisDef(id="move_dataset", parameters= {"source_dataset_id", "target_dataset_id"})
	public static SystemAnalysis moveDataSet(String id, String srcDsId, String tarDsId) {
		List<String> args = FStream.of(srcDsId, tarDsId).toList();
		
		return new SystemAnalysis(id, "move_dataset", args);
	}

	@SystemAnalysisDef(id="create_thumbnail", parameters= {"dataset_id", "sample_count"})
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
