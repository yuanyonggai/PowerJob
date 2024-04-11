package tech.powerjob.official.processors.impl.java.executable;

import groovy.lang.GroovyClassLoader;
import tech.powerjob.official.processors.impl.java.executable.impl.SpringExecutableResourceFactory;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;

/**
 * executable factory, product class/object by name
 *
 */
public class ExecutableResourceFactory {

	private static ExecutableResourceFactory executableFactory = new ExecutableResourceFactory();

	public static ExecutableResourceFactory getInstance() {
		return executableFactory;
	}

	public static void refreshInstance(int type) {
		if (type == 0) {
			executableFactory = new ExecutableResourceFactory();
		} else if (type == 1) {
			executableFactory = new SpringExecutableResourceFactory();
		}
	}

	/**
	 * groovy class loader
	 */
	private GroovyClassLoader groovyClassLoader = new GroovyClassLoader();

	/**
	 * load new instance, prototype
	 *
	 * @param codeSource
	 * @return
	 * @throws Exception
	 */
	public BasicProcessor loadNewInstance(String codeSource) throws Exception {
		if (codeSource != null && codeSource.trim().length() > 0) {
			Class<?> clazz = groovyClassLoader.parseClass(codeSource);
			if (clazz != null) {
				Object instance = clazz.getDeclaredConstructor().newInstance();
				if (instance != null) {
					if (instance instanceof BasicProcessor) {
						this.injectService(instance);
						return (BasicProcessor) instance;
					} else {
						throw new IllegalArgumentException(
								">>>>>>>>>>> ExecutableResourceFactory, loadNewInstance error, cannot convert from instance["
										+ instance.getClass() + "] to BasicProcessor");
					}
				}
			}
		}
		throw new IllegalArgumentException(
				">>>>>>>>>>> ExecutableResourceFactory, loadNewInstance error, codeSource is null");
	}

	/**
	 * inject service of bean field
	 *
	 * @param instance
	 */
	public void injectService(Object instance) {
		// do something
	}

}
