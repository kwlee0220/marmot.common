package marmot.remote.protobuf;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBRemoteException extends RuntimeException {
	private static final long serialVersionUID = 5193983213734027997L;
	
	public PBRemoteException(String deatils) {
		super(deatils);
	}

}
