package marmot;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.proto.optor.RecordScriptProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;
import utils.script.MVELScript.ImportClass;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class RecordScript implements PBSerializable<RecordScriptProto> {
	private final String m_script;
	private final FOption<String> m_initializer;
	private final List<ImportClass> m_importedClasses = Lists.newArrayList();
	private final Map<String,Object> m_arguments = Maps.newHashMap();
	
	public static RecordScript of(String expr) {
		return new RecordScript(expr);
	}
	
	public static RecordScript of(String init, String expr) {
		return new RecordScript(init, expr);
	}

	private RecordScript(String script) {
		Utilities.checkNotNullArgument(script, "script is null");
		
		m_script = script;
		m_initializer = FOption.empty();
	}

	private RecordScript(String initScript, String script) {
		Utilities.checkNotNullArgument(initScript, "initialization script is null");
		Utilities.checkNotNullArgument(script, "script is null");

		m_initializer = FOption.of(initScript);
		m_script = script;
	}
	
	public String getScript() {
		return m_script;
	}
	
	public FOption<String> getInitializer() {
		return m_initializer;
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
	
	public List<ImportClass> getImportedClassAll() {
		return Collections.unmodifiableList(m_importedClasses);
	}
	
	public RecordScript importClass(ImportClass ic) {
		Utilities.checkNotNullArgument(ic, "ImportedClass is null");
		
		m_importedClasses.add(ic);
		return this;
	}
	
	public RecordScript importClass(Class<?> cls) {
		Utilities.checkNotNullArgument(cls, "ImportedClass is null");
		
		m_importedClasses.add(new ImportClass(cls));
		return this;
	}
	
	public RecordScript importClass(Class<?> cls, String name) {
		Utilities.checkNotNullArgument(cls, "ImportedClass is null");
		
		m_importedClasses.add(new ImportClass(cls, name));
		return this;
	}

	public static RecordScript fromProto(RecordScriptProto proto) {
		RecordScript rscript = null;
		switch ( proto.getOptionalInitializerCase() ) {
			case INITIALIZER:
				rscript = RecordScript.of(proto.getInitializer(), proto.getExpr());
				break;
			case OPTIONALINITIALIZER_NOT_SET:
				rscript = new RecordScript(proto.getExpr());
				break;
			default:
				throw new AssertionError();
		}
		
		RecordScript frscript = rscript;
		switch ( proto.getOptionalArgumentsCase() ) {
			case ARGUMENTS:
				PBUtils.fromProto(proto.getArguments())
						.forEach((k,v) -> frscript.addArgument(k, v));
				break;
			default:
		}
		
		FStream.from(proto.getImportedClassList())
				.map(ImportClass::parse)
				.forEach(frscript::importClass);
		
		return frscript;
	}

	@Override
	public RecordScriptProto toProto() {
		List<String> importeds = FStream.from(getImportedClassAll())
										.map(ImportClass::toString)
										.toList();
		RecordScriptProto.Builder builder = RecordScriptProto.newBuilder()
														.setExpr(getScript())
														.addAllImportedClass(importeds);
		m_initializer.ifPresent(builder::setInitializer);
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
