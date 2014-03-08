package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import org.apache.hadoop.hbase.regionserver.HRegion;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;

public abstract class AbstractPluggableIndex implements Serializable {

	private static final long serialVersionUID = -3578521804781335557L;

	

	// Return an instance of the object
	public static AbstractPluggableIndex getInstance(String indexType,
			Object[] arguments) {
		return (AbstractPluggableIndex) createObject(indexType, arguments);
	}

	private static Object createObject(String indexType, Object[] arguments) {

		Object object = null;
		Class classDefinition;
		try {
			classDefinition = Class.forName(indexType);
			try {
				// We assume that the class only has one constructor
				object = classDefinition.getConstructors()[0]
						.newInstance(arguments);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return object;
	}

	public abstract void add(byte[] key, byte[] value);

	public abstract byte[][] get(byte[] key);

	public abstract void fullBuild(HRegion region);

	public abstract void removeValueFromIdx(byte[] key, byte[] value);

	public abstract Set<byte[]> filterRowsFromCriteria(Criterion<?> criterion);

}
