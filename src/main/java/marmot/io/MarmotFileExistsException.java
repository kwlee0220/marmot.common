package marmot.io;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotFileExistsException extends MarmotFileException {
	private static final long serialVersionUID = -5218021052993352476L;

	public MarmotFileExistsException(String msg) {
		super(msg);
	}
}
