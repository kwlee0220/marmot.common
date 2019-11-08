package marmot.plan;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Map;

import org.mvel2.templates.TemplateRuntime;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;

import marmot.Plan;
import utils.func.Lazy;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class STScriptPlanLoader {
	private static final Map<?,?> VARS = Maps.newHashMap();
	private static final String TEMPLATE_GROUP_RESOURCE = "marmot/plan/operator_templates.stg";
	private static Lazy<STGroup> s_tmpltGroup = Lazy.of(STScriptPlanLoader::loadTemplateGroup);
	
	public static Plan load(URL url, Charset charset) throws IOException {
		try ( InputStream is = url.openStream() ) {
			String script = IOUtils.toString(is, charset);
			return load(script);
		}
	}
	
	public static Plan load(File file) throws IOException {
		return load(IOUtils.toString(file));
	}
	
	public static Plan loadFromResource(String name) throws IOException {
		return loadFromResource(name, Charset.defaultCharset());
	}
	
	public static Plan loadFromResource(String name, Charset charset) throws IOException {
		try ( InputStream is = Thread.currentThread()
									.getContextClassLoader()
									.getResourceAsStream(name) ) {
			if ( is == null ) {
				throw new IllegalArgumentException("invalid STScript: resource name=" + name);
			}
			String script = IOUtils.toString(is, charset);
			return load(script);
		}
	}
	
	public static Plan load(Reader reader) throws IOException {
		return loadFromResource(IOUtils.toString(reader));	
	}
	
	public static Plan load(InputStream is, Charset charset) throws IOException {
		String script = IOUtils.toString(is, charset);
		return load(script);
	}
	
	public static String parseToPlanJson(String script) {
		// MVEL template 엔진을 써서 1차 변환함. 
		script = (String)TemplateRuntime.eval(script, VARS);
		
		// StringTemplate을 통해 연산 정의를 JSON 정의 포맷으로 변환시킴
		return new ST(s_tmpltGroup.get(), script).render();
	}
	
	public static Plan load(String script) throws InvalidProtocolBufferException {
		// MVEL template 엔진을 써서 1차 변환함. 
		script = (String)TemplateRuntime.eval(script, VARS);
		
		// StringTemplate을 통해 연산 정의를 JSON 정의 포맷으로 변환시킴
		String json = new ST(s_tmpltGroup.get(), script).render();
		
		// JSON으로 기술된 plan 정의를 읽어 Plan 객체를 생성함.
		return Plan.parseJson(json);
	}
	
	private static final STGroup loadTemplateGroup() {
		URL url = STScriptPlanLoader.class.getClassLoader().getResource(TEMPLATE_GROUP_RESOURCE);
		return new STGroupFile(url, "utf-8", '<', '>');
	}
}
