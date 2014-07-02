package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

import org.apache.hadoop.hbase.regionserver.HRegion;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion;

public abstract class AbstractPluggableIndex implements Serializable {

	private static final long serialVersionUID = -3578521804781335557L;
	private String indexType;
	private Object[] arguments;
	private Class<?> [] argumentsClasses;
	private boolean isMultiColumn;

	// Return an instance of the object
	public synchronized static AbstractPluggableIndex getInstance(
			boolean isMultiColumn, String indexType, Object[] arguments, Class<?> [] argumentsClasses) throws NoSuchMethodException {
		AbstractPluggableIndex absIndex = (AbstractPluggableIndex) createObject(
				isMultiColumn, indexType, arguments, argumentsClasses);
		absIndex.setArguments(arguments);
		absIndex.setIndexType(indexType);
		absIndex.setIsMultiColumn(isMultiColumn);
		return absIndex;
	}

	private static Object createObject(boolean isMultiColumn, String indexType, Object[] arguments, Class<?> [] argumentsClasses) {

		Object object = null;
		Class<?> classDefinition;
		try {
			classDefinition = Class.forName(indexType);
			try {
				// We assume the first constructor is for single column indexing
				// the second constructor is for multi column indexing
				// a better solution is to pass class array object
//				if(isMultiColumn) {
//					object = classDefinition.getConstructors()[0]
//							.newInstance(arguments);
//				} else {
//					object = classDefinition.getConstructors()[1]
//							.newInstance(arguments);
//				}
				
				object = classDefinition.getConstructor(argumentsClasses).newInstance(arguments);
				
			
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return object;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	public void setArguments(Object[] arguments) {
		this.arguments = arguments;
	}
	
	public void setArgumentsClasses(Class<?>[] argumentsClasses) {
		this.argumentsClasses = argumentsClasses;
	}
	
	public void setIsMultiColumn(boolean isMultiCol) {
		this.isMultiColumn = isMultiCol;
	}

	public boolean getIsMultiColumn() {
		return this.isMultiColumn;
	}
	
	public String getIndexType() {
		return this.indexType;
	}

	public Object[] getArguments() {
		return this.arguments;
	}
	
	public Class<?>[] getArgumentsClasses() {
		return this.argumentsClasses;
	}

	public abstract void add(byte[] key, byte[] value);

	public abstract byte[][] get(byte[] key);

	public abstract void fullBuild(HRegion region);

	public abstract void removeValueFromIdx(byte[] key, byte[] value);

	public abstract Set<byte[]> filterRowsFromCriteria(Criterion<?> criterion);

	public abstract void split(AbstractPluggableIndex daughterRegionA,
			AbstractPluggableIndex daughterRegionB, byte[] splitRow);

}
