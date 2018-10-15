package marmot.workbench;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Operator {
	public String name();
	public String protoId();
	public OperatorType type() default OperatorType.NORMAL;
	public String description() default "no description";
}
