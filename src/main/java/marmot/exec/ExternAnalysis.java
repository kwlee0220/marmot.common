package marmot.exec;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisProto.ExternExecProto;
import marmot.proto.service.MarmotAnalysisProto.MemberCase;
import utils.Utilities;
import utils.io.FileUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExternAnalysis extends MarmotAnalysis {
	private final String m_execPath;
	private final List<String> m_args;
	
	public ExternAnalysis(String id, String execPath, List<String> args) {
		super(id, Type.EXTERN);
		
		m_execPath = execPath;
		m_args = args;
	}
	
	public ExternAnalysis(String id, String execPath, String... args) {
		super(id, Type.EXTERN);
		
		m_execPath = execPath;
		m_args = Arrays.asList(args);
	}
	
	public String getExecPath() {
		return m_execPath;
	}
	
	public List<String> getArguments() {
		return m_args;
	}
	
	@Override
	public String toString() {
		String argStr = FStream.from(m_args).join(" ");
		return String.format("%s[%s]: %s %s", getType(), getId(), m_execPath, argStr);
	}
	
	public static ExternAnalysis fromProto(MarmotAnalysisProto proto) {
		Utilities.checkArgument(proto.getMemberCase().equals(MemberCase.EXTERN_EXEC),
								"not ExternAnalysis");
		
		ExternExecProto sysFunc = proto.getExternExec();
		List<String> args = sysFunc.getArgumentsList();
		
		return new ExternAnalysis(proto.getId(), sysFunc.getExecPath(), args);
	}

	@Override
	public MarmotAnalysisProto toProto() {
		ExternExecProto sysFunc = ExternExecProto.newBuilder()
												.setExecPath(m_execPath)
												.addAllArguments(m_args)
												.build();
		return MarmotAnalysisProto.newBuilder()
									.setId(getId())
									.setExternExec(sysFunc)
									.build();
	}
	
	public static ExternAnalysis exportAsCsv(String id, String dsId, File csvFile, String[] args) {
		List<String> argList = Lists.newArrayList(dsId, csvFile.getAbsolutePath(), "export", "csv");
		argList.addAll(Arrays.asList(args));
		
		String execPath = FileUtils.findExecutable("mc_dataset");
		return new ExternAnalysis(id, execPath, argList);
	}
}
