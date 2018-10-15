package marmot.plan;

import java.util.List;
import java.util.Map;

import org.mvel2.integration.VariableResolverFactory;

import com.google.common.collect.Maps;

import io.vavr.control.Option;
import marmot.proto.optor.RecordScriptProto;
import marmot.protobuf.PBUtils;
import marmot.support.MVELScript;
import marmot.support.MVELScript.ImportedClass;
import marmot.support.PBSerializable;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordScript implements PBSerializable<RecordScriptProto> {
	private final MVELScript m_script;
	private Option<MVELScript> m_initializer;
	private final Map<String,Object> m_arguments = Maps.newHashMap();
	
	public static RecordScript of(String expr) {
		return new RecordScript(expr);
	}
	
	public static RecordScript of(String init, String expr) {
		return new RecordScript(init, expr);
	}

	private RecordScript(String expr) {
		m_script = MVELScript.of(expr);
		m_initializer = Option.none();
	}

	private RecordScript(String init, String expr) {
		m_script = MVELScript.of(expr);
		m_initializer = Option.some(MVELScript.of(init));
	}
	
	public String getExpression() {
		return m_script.getScript();
	}
	
	public Option<String> getInitializer() {
		return m_initializer.map(MVELScript::getScript);
	}
	
	public RecordScript setInitializer(String expr) {
		m_initializer = Option.of(expr).map(MVELScript::of);
		m_initializer.forEach(init -> m_script.getImportedClasses()
												.stream()
												.forEach(init::importClass));
		return this;
	}
	
	public Map<String,Object> getArgumentAll() {
		return m_arguments;
	}
	
	public RecordScript addArgument(String name, Object value) {
		m_arguments.put(name, value);
		return this;
	}
	
	public RecordScript addArgumentAll(Map<String,Object> args) {
		args.forEach((k,v) -> addArgument(k, v));
		return this;
	}
	
	public List<ImportedClass> getImportedClassAll() {
		return m_script.getImportedClasses();
	}
	
	public RecordScript importClass(String clsName) {
		ImportedClass ic = ImportedClass.parse(clsName);
		m_script.importClass(ic);
		m_initializer.forEach(script -> script.importClass(ic));
		
		return this;
	}
	
	public RecordScript importClass(Class<?> cls) {
		m_script.importClass(cls);
		m_initializer.forEach(script -> script.importClass(cls));
		
		return this;
	}
	
	public RecordScript importFunctionAll(Class<?> funcCls) {
		m_script.importFunctionAll(funcCls);
		m_initializer.forEach(script -> script.importClass(funcCls));
		
		return this;
	}
	
	public void initialize(Map<String, Object> vars) {
		m_initializer.forEach(init -> init.execute(vars));
	}
	
	public void initialize(VariableResolverFactory resolverFact) {
		m_initializer.forEach(init -> init.execute(resolverFact));
	}
	
	public Object execute(Map<String, Object> vars) {
		return m_script.execute(vars);
	}
	
	public Object execute(VariableResolverFactory resolverFact) {
		return m_script.execute(resolverFact);
	}

	public static RecordScript fromProto(RecordScriptProto proto) {
		RecordScript expr = new RecordScript(proto.getExpr());
		
		switch ( proto.getOptionalInitializerCase() ) {
			case INITIALIZER:
				expr.setInitializer(proto.getInitializer());
				break;
			default:
		}
		switch ( proto.getOptionalArgumentsCase() ) {
			case ARGUMENTS:
				PBUtils.fromProto(proto.getArguments())
						.forEach((k,v) -> expr.addArgument(k, v));
				break;
			default:
		}
		
		proto.getImportedClassList().forEach(expr::importClass);
		
		return expr;
	}

	@Override
	public RecordScriptProto toProto() {
		List<String> importeds = FStream.of(getImportedClassAll())
										.map(ImportedClass::toString)
										.toList();
		RecordScriptProto.Builder builder = RecordScriptProto.newBuilder()
														.setExpr(getExpression())
														.addAllImportedClass(importeds);
		m_initializer.map(MVELScript::getScript).forEach(builder::setInitializer);
		if ( !m_arguments.isEmpty() ) {
			builder.setArguments(PBUtils.toKeyValueMapProto(m_arguments));
		}
		
		return builder.build();
	}
	
	@Override
	public String toString() {
		return m_script.toString();
	}
}
