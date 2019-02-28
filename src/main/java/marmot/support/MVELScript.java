package marmot.support;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.mvel2.MVEL;
import org.mvel2.ParserContext;
import org.mvel2.integration.VariableResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MVELScript {
	private static final Logger s_logger = LoggerFactory.getLogger(MVELScript.class);
	public static final String ELEMENT_NAME = "script_descriptor";
	
	private ParserContext m_pc;
	private String m_scriptExpr;
	private volatile Serializable m_compiled;
	private List<ImportedClass> m_importedClasses = Lists.newArrayList();
	
	public static MVELScript of(String scriptStr) {
		Preconditions.checkArgument(scriptStr != null);
		
		return new MVELScript(scriptStr);
	}
	
	private MVELScript(String expr) {
		m_pc = initializeContext();
		
		m_scriptExpr = expr;
		m_compiled = MVEL.compileExpression(m_scriptExpr, m_pc);
	}
	
	public String getScript() {
		return m_scriptExpr;
	}
	
	public List<ImportedClass> getImportedClasses() {
		return Collections.unmodifiableList(m_importedClasses);
	}
	
	public final ParserContext getParserContext() {
		return m_pc;
	}
	
	public void importClass(ImportedClass ic) {
		if ( ic.m_name.isPresent() ) {
			m_pc.addImport(ic.m_name.get(), ic.m_class);
		}
		else {
			m_pc.addImport(ic.m_class);
		}
		
		m_importedClasses.add(ic);
	}
	
	public void importClass(Class<?> cls) {
		m_pc.addImport(cls);
		m_importedClasses.add(new ImportedClass(cls));
	}
	
	public void importClass(Class<?> cls, String name) {
		if ( name != null ) {
			m_pc.addImport(name, cls);
		}
		else {
			m_pc.addImport(cls);
		}
		
		m_importedClasses.add(new ImportedClass(cls, name));
	}
	
	public void importFunctionAll(Class<?> funcCls) {
		importFunctions(m_pc, funcCls);
	}
	
	public MVELScript compile() {
		m_compiled = MVEL.compileExpression(m_scriptExpr, m_pc);
		return this;
	}

	public Object execute(Map<String, Object> vars) {
		if ( m_compiled == null ) {
			compile();
		}
		return MVEL.executeExpression(m_compiled, vars);
	}

	public Object execute(VariableResolverFactory resolverFact) {
		if ( m_compiled == null ) {
			compile();
		}
		return MVEL.executeExpression(m_compiled, resolverFact);
	}
	
	public static List<ImportedClass> parseImportClassesProto(List<String> icProto) {
		List<ImportedClass> icList = Lists.newArrayList();
		for ( String ic: icProto ) {
			icList.add(ImportedClass.parse(ic));
		}
		
		return icList;
	}
	
	public static List<String> toImportClassesProto(List<ImportedClass> icList) {
		return FStream.from(icList)
						.map(ImportedClass::toString)
						.toList();
	}
	
	@Override
	public String toString() {
		return m_scriptExpr;
	}
	
	private ParserContext initializeContext() {
		try {
			ParserContext pc = ParserContext.create();

			importFunctions(pc, Functions.class);
//			importFunctions(pc, GeoFunctions.class);
			importFunctions(pc, DateTimeFunctions.class);
			importFunctions(pc, DateFunctions.class);
			importFunctions(pc, TimeFunctions.class);
			importFunctions(pc, TrajectoryFunctions.class);
			importFunctions(pc, IntervalFunctions.class);
			importFunctions(pc, MVELFunctions.class);
			importFunctions(pc, JsonParser.class);

			pc.addPackageImport("java.util");
			pc.addPackageImport("com.google.common.collect");
			
			return pc;
		}
		catch ( Exception e ) {
			throw new ScriptException(e);
		}
	}
	
	private static void importFunctions(ParserContext pc, Class<?> cls) {
		for ( Method method: cls.getDeclaredMethods() ) {
			MVELFunction func = method.getAnnotation(MVELFunction.class);
			if ( func != null ) {
				s_logger.debug("importing MVEL function: name={} method={}", func.name(), method);
				pc.addImport(func.name(), method);
			}
		}
	}
	
	public static class ImportedClass {
		private final Class<?> m_class;
		private final FOption<String> m_name;
		
		ImportedClass(Class<?> cls, String name) {
			m_class = cls;
			m_name = FOption.of(name);
		}
		
		ImportedClass(Class<?> cls) {
			m_class = cls;
			m_name = FOption.empty();
		}
		
		public Class<?> getImportClass() {
			return m_class;
		}
		
		public FOption<String> getImportName() {
			return m_name;
		}
		
		public static ImportedClass parse(String str) {
			String[] parts = str.split(":");
			
			try {
				Class<?> cls = Class.forName(parts[0]);
				if ( parts.length == 2 ) {
					String name = parts[1].trim();
					return new ImportedClass(cls, name);
				}
				else {
					return new ImportedClass(cls);
				}
			}
			catch ( ClassNotFoundException e ) {
				throw new IllegalArgumentException(""+e);
			}
		}
		
		@Override
		public String toString() {
			String clsName = m_class.getName();
			return m_name.map(name -> String.format("%s:%s", clsName, name))
						.getOrElse(() -> "" + clsName);
		}
	}
}