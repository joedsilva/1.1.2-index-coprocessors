package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;

import ca.mcgill.distsys.hbase96.indexcommons.proto.Criterion;

public abstract class AbstractPluggableIndex implements Serializable {

	private static final Log LOG =
			LogFactory.getLog(AbstractPluggableIndex.class);

	private static final long serialVersionUID = -3578521804781335557L;

	private String indexType;
	private Object[] arguments;

	// Return an instance of the object
	public synchronized static AbstractPluggableIndex getInstance(
			String indexType, Object[] arguments)
			throws NoSuchMethodException {
		AbstractPluggableIndex absIndex = (AbstractPluggableIndex)
				createObject(indexType, arguments);
		absIndex.setArguments(arguments);
		absIndex.setIndexType(indexType);
		return absIndex;
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
