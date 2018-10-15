package marmot.plan;

import java.lang.reflect.Method;

import org.apache.commons.lang3.reflect.MethodUtils;

import marmot.PlanBuilder;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PlanBuilderEnhancer<T extends PlanBuilder> {
	private final PlanBuilder m_planBuilder;
	
	public static <T extends PlanBuilder> PlanBuilderEnhancer<T> from(PlanBuilder planBuilder) {
		return new PlanBuilderEnhancer<>(planBuilder);
	}
	
	private PlanBuilderEnhancer(PlanBuilder planBuilder) {
		m_planBuilder = planBuilder;
	}
	
	@SuppressWarnings("unchecked")
	public T enhanceTo(Class<T> targetCls) {
		Enhancer enhancer = new Enhancer();
		enhancer.setSuperclass(targetCls);
		enhancer.setCallback(new MethodInterceptor() {
			@Override
			public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
				Class<?> declCls = method.getDeclaringClass();
				
				if ( declCls.equals(PlanBuilder.class) ) {
					PlanBuilder builder = (PlanBuilder)MethodUtils.invokeExactMethod(obj, "end");
					return proxy.invoke(builder, args);
				}
				else if ( targetCls.isAssignableFrom(declCls) ) {
					return proxy.invokeSuper(obj, args);
				}
				else if ( PlanBuilder.class.isAssignableFrom(declCls) ) {
					return proxy.invoke(m_planBuilder, args);
				}
				
				throw new AssertionError();
			}
		});
		return (T)enhancer.create(new Class[] {PlanBuilder.class},
									new Object[] {m_planBuilder});
	}
}
