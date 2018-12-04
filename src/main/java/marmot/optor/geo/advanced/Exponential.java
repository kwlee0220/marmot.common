package marmot.optor.geo.advanced;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Exponential extends WeightFunction {
	private final double m_beta;
	
	public static Exponential of(String paramStr) {
		double beta = Double.parseDouble(paramStr);
		
		return new Exponential(beta);
	}
	
	public static Exponential of(double beta) {
		return new Exponential(beta);
	}
	
	private  Exponential(double beta) {
		m_beta = beta;
	}

	@Override
	public double apply(double x) {
		return Math.exp(m_beta * x);
	}

	@Override
	public String toStringExpr() {
		return String.format("exponential:%f", m_beta);
	}
}
