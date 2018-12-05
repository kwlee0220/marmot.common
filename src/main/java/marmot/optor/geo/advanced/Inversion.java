package marmot.optor.geo.advanced;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Inversion extends WeightFunction {
	private final WeightFunction m_wfunc;
	
	public static Inversion ofParameter(String paramStr) {
		WeightFunction wfunc = WeightFunction.fromString(paramStr);
		
		return new Inversion(wfunc);
	}
	
	public static Inversion of(WeightFunction wfunc) {
		return new Inversion(wfunc);
	}
	
	private  Inversion(WeightFunction wfunc) {
		m_wfunc = wfunc;
	}

	@Override
	public double apply(double x) {
		return 1 / m_wfunc.apply(x);
	}

	@Override
	public String toStringExpr() {
		return String.format("inversion:" + m_wfunc.toStringExpr());
	}
}
