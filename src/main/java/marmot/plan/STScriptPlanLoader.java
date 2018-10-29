package marmot.plan;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

import org.mvel2.templates.TemplateRuntime;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

import io.vavr.Lazy;
import marmot.Plan;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class STScriptPlanLoader {
	private static final Map VARS = Maps.newHashMap();
	private static final String TEMPLATE_GROUP_RESOURCE = "marmot/plan/operator_templates.stg";
	private static Lazy<STGroup> s_tmpltGroup = Lazy.of(STScriptPlanLoader::loadTemplateGroup);
	
	public static Plan load(URL url, Charset charset) throws IOException {
		try ( InputStream is = url.openStream() ) {
			String script = IOUtils.toString(is, charset);
			return parseScript(script);
		}
	}
	
	public static Plan load(File file) throws IOException {
		return parseScript(IOUtils.toString(file));
	}
	
	public static Plan load(File file, Charset charset) throws IOException {
		return parseScript(IOUtils.toString(file, charset));
	}
	
	public static Plan load(String name) throws IOException {
		return load(name, Charset.defaultCharset());
	}
	
	public static Plan load(String name, Charset charset) throws IOException {
		try ( InputStream is = Thread.currentThread()
									.getContextClassLoader()
									.getResourceAsStream(name) ) {
			if ( is == null ) {
				throw new IllegalArgumentException("invalid STScript: resource name=" + name);
			}
			String script = IOUtils.toString(is, charset);
			return parseScript(script);
		}
	}
	
	public static String toJson(String script) {
		// MVEL template 엔진을 써서 1차 변환함. 
		script = (String)TemplateRuntime.eval(script, VARS);
		
		return new ST(s_tmpltGroup.get(), script).render();
	}
	
	private static Plan parseScript(String script) throws InvalidProtocolBufferException {
		String json = new ST(s_tmpltGroup.get(), script).render();
		return Plan.parseJson(json);
	}
	
	private static final STGroup loadTemplateGroup() {
		URL url = STScriptPlanLoader.class.getClassLoader().getResource(TEMPLATE_GROUP_RESOURCE);
		return new STGroupFile(url, "utf-8", '<', '>');
	}
}
