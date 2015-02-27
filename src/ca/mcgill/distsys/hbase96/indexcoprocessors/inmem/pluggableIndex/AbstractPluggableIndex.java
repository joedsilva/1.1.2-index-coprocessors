package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;

import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;

public abstract class AbstractPluggableIndex implements Serializable {

	private static final Log LOG = LogFactory
			.getLog(AbstractPluggableIndex.class);

	private static final long serialVersionUID = -3578521804781335557L;

	private String indexType;
	private Object[] arguments;

	// Return an instance of the object
	public synchronized static AbstractPluggableIndex getInstance(
			String indexType, Object[] arguments) throws NoSuchMethodException {
		AbstractPluggableIndex absIndex = (AbstractPluggableIndex) createObject(
				indexType, arguments);
		absIndex.setArguments(arguments);
		absIndex.setIndexType(indexType);
		return absIndex;
	}

	private static <C> Constructor<C> getAppropriateConstructor(
			Class<C> constructorClass, Object[] arguments) {

		if (arguments == null) {
			arguments = new Object[0];
		}

		// check for each constructor
		for (Constructor con : constructorClass.getDeclaredConstructors()) {

			Class[] paramTypes = con.getParameterTypes();
			if (paramTypes.length != arguments.length) {
				continue;
			}
			boolean match = true;
			// check for each parameter type with the argument type
			for (int i = 0; i < arguments.length; i++) {

				Class target = arguments[i].getClass();
				Class actual = paramTypes[i];
				// check if the constructor parameter contains primitive type
				if (!actual.isAssignableFrom(target)) {
					if (actual.isPrimitive()) {
						match = (actual.equals(int.class) && target
								.equals(Integer.class))
								|| (actual.equals(double.class) && target
										.equals(Double.class))
								|| (actual.equals(float.class) && target
										.equals(Float.class))
								|| (actual.equals(long.class) && target
										.equals(Long.class))
								|| (actual.equals(char.class) && target
										.equals(Character.class))
								|| (actual.equals(short.class) && target
										.equals(Short.class))
								|| (actual.equals(boolean.class) && target
										.equals(Boolean.class))
								|| (actual.equals(byte.class) && target
										.equals(Byte.class));
					} else {
						match = false;
					}
				}
				if (match == false) {
					break;
				}

			}

			if (match == true) {
				return con;
			}
		}

		// return null if no matching constructor is found
		return null;
	}

	private static Object createObject(String indexType, Object[] arguments) {
		Object object = null;
		Class<?> classDefinition;
		try {
			classDefinition = Class.forName(indexType);
			try {
				 object = classDefinition.
				 getConstructor(new Class[]{Object[].class}).
				 newInstance(new Object[]{arguments});
//				Constructor con = getAppropriateConstructor(classDefinition,
////						arguments);
//				if (con == null) {
//					throw new IllegalArgumentException(
//							"Can't find appropriate constructor for class "
//									+ indexType + " and arguments "
//									+ Arrays.toString(arguments));
//				}
//				object = con.newInstance(arguments);
			} catch (Exception e) {
				LOG.error(e);
			}
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		}
		return object;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	public void setArguments(Object[] arguments) {
		this.arguments = arguments;
	}

	public String getIndexType() {
		return this.indexType;
	}

	public Object[] getArguments() {
		return this.arguments;
	}

	public abstract void add(byte[] key, byte[] value);

	public abstract byte[][] get(byte[] key);

	public abstract void fullBuild(HRegion region);

	public abstract void remove(byte[] key, byte[] value);

	public abstract Set<byte[]> filterRowsFromCriteria(Criterion<?> criterion);

	public abstract void split(AbstractPluggableIndex daughterRegionA,
			AbstractPluggableIndex daughterRegionB, byte[] splitRow);

}
