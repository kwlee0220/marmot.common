package marmot.remote.protobuf;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBStreamClosedException extends PBProtocolException {
	private static final long serialVersionUID = -4220592571399231727L;

	public PBStreamClosedException(String deatils) {
		super(deatils);
	}
}
