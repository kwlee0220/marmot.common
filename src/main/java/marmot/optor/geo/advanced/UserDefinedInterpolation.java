package marmot.optor.geo.advanced;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.mvel2.MVEL;
import org.mvel2.ParserContext;
import org.mvel2.integration.VariableResolverFactory;
import org.mvel2.integration.impl.MapVariableResolverFactory;

import com.google.common.collect.Maps;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class UserDefinedInterpolation extends InterpolationMethod {
	private final String m_udfExpr;
	private final VariableResolverFactory m_funcFact;
	private final Map<String,Object> m_vars = Maps.newHashMap();
	private final Serializable m_compiled;
	
	public static UserDefinedInterpolation using(String expr) {
		return new UserDefinedInterpolation(expr);
	}
	
	UserDefinedInterpolation(String udfExpr) {
		m_udfExpr= udfExpr;
		m_funcFact = new MapVariableResolverFactory();
		MVEL.eval(udfExpr, m_funcFact);
		ParserContext cntxt = new ParserContext();
		m_compiled = MVEL.compileExpression("interpolate(factors)", cntxt);
	}

	@Override
	public double interpolate(List<SpatialFactor> factors) {
		m_vars.put("factors", factors);
		return (double)MVEL.executeExpression(m_compiled, m_vars, m_funcFact);
	}

	@Override
	public String toStringExpr() {
		return "user_defined:" + m_udfExpr;
	}
}
