package marmot.optor.geo.advanced;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Power extends WeightFunction {
	private final double m_beta;
	
	public static Power of(String paramStr) {
		double beta = Double.parseDouble(paramStr);
		
		return new Power(beta);
	}
	
	public static Power using(double beta) {
		return new Power(beta);
	}
	
	private  Power(double beta) {
		m_beta = beta;
	}

	@Override
	public double apply(double x) {
		return Math.pow(x, m_beta);
	}

	@Override
	public String toStringExpr() {
		return String.format("power:%f", m_beta);
	}
}
