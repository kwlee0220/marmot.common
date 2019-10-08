package marmot.exec;

import java.util.List;

import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisProto.CompositeExecProto;
import marmot.proto.service.MarmotAnalysisProto.MemberCase;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CompositeAnalysis extends MarmotAnalysis {
	private final List<String> m_components;
	
	public CompositeAnalysis(String id, List<String> components) {
		super(id, Type.COMPOSITE);

		m_components = components;
	}
	
	public List<String> getComponents() {
		return m_components;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new  StringBuilder();
		builder.append(String.format("%s[%s]:%n", getType(), getId()));
		for ( int i =0; i < m_components.size(); ++i ) {
			builder.append(String.format("   %02d: %s%n", i+1, m_components.get(i)));
		}
		
		return builder.toString();
	}
	
	public static CompositeAnalysis fromProto(MarmotAnalysisProto proto) {
		Utilities.checkArgument(proto.getMemberCase().equals(MemberCase.COMPOSITE_EXEC),
								"not CompositeAnalytics");
		
		List<String> comps = proto.getCompositeExec().getComponentList();
		return new CompositeAnalysis(proto.getId(), comps);
	}

	@Override
	public MarmotAnalysisProto toProto() {
		CompositeExecProto composite = CompositeExecProto.newBuilder()
														.addAllComponent(m_components)
														.build();
		return MarmotAnalysisProto.newBuilder()
									.setId(getId())
									.setCompositeExec(composite)
									.build();
	}
}
