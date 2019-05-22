package marmot.externio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import utils.UnitUtils;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExternIoUtils {
	private ExternIoUtils() {
		throw new AssertionError("should not be called: class=" + ExternIoUtils.class);
	}
	
	private static final int DEFAULT_BUFFER_SIZE = (int)UnitUtils.parseByteSize("32kb");
	public static BufferedWriter toWriter(FOption<String> path, Charset charset)
		throws IOException {
		OutputStream os = path.mapTE(str -> (OutputStream)new FileOutputStream(new File(str)))
								.getOrElse(System.out);
	    return new BufferedWriter(new OutputStreamWriter(os, charset), DEFAULT_BUFFER_SIZE);
	}
}
