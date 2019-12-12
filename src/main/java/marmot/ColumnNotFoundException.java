package marmot;

import marmot.dataset.DataSetException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ColumnNotFoundException extends DataSetException {
	private static final long serialVersionUID = -3157374552897214944L;

	public ColumnNotFoundException(String name) {
		super(name);
	}
}
