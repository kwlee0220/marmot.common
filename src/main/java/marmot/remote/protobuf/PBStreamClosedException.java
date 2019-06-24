package marmot.remote.protobuf;

import marmot.support.PBException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBStreamClosedException extends PBException {
	private static final long serialVersionUID = -4220592571399231727L;

	public PBStreamClosedException(String deatils) {
		super(deatils);
	}
}
