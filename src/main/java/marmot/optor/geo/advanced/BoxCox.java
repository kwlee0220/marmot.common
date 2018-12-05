package marmot.optor.geo.advanced;

import java.util.List;

import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BoxCox extends WeightFunction {
	private final double m_beta;
	private final double m_lambda;
	
	public static BoxCox ofParameter(String paramStr) {
		List<String> parts = CSV.parse(paramStr, ',', '\\');
		double beta = Double.parseDouble(parts.get(0));
		double lambda = Double.parseDouble(parts.get(1));
		
		return new BoxCox(beta, lambda);
	}
	
	public static BoxCox of(double beta, double lambda) {
		return new BoxCox(beta, lambda);
	}
	
	private  BoxCox(double beta, double lambda) {
		m_beta = beta;
		m_lambda = lambda;
	}

	@Override
	public double apply(double x) {
		if ( m_lambda != 0 ) {
			return Math.exp(m_beta * ((Math.pow(x, m_lambda) - 1) / m_lambda));
		}
		else {
			return Math.pow(x, m_beta);
		}
	}

	@Override
	public String toStringExpr() {
		return String.format("box_cox:%f,%f", m_beta, m_lambda);
	}
}
