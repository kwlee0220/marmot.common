package marmot.externio;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;

import io.vavr.control.Option;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ExternIoUtils {
	private ExternIoUtils() {
		throw new AssertionError("should not be called: class=" + ExternIoUtils.class);
	}
	
	private static final int DEFAULT_BUFFER_SIZE = (int)UnitUtils.parseByteSize("32kb");
	public static BufferedWriter toWriter(Option<String> output, Charset charset)
		throws IOException {
		if ( output.isDefined() ) {
			File file = new File(output.get());
		    return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), charset));
		}
		else {
			return new BufferedWriter(new OutputStreamWriter(System.out, charset),
									DEFAULT_BUFFER_SIZE);
		}
	}
}
