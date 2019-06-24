package marmot.remote.protobuf;

import marmot.support.PBException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBRemoteException extends PBException {
	private static final long serialVersionUID = 5193983213734027997L;
	
	public PBRemoteException(String deatils) {
		super(deatils);
	}

}
