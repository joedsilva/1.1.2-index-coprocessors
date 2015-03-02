package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;
import java.io.Serializable;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.test.*;

public abstract class DeepCopyObject implements Comparable<DeepCopyObject>, Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 104804362850313367L;

	public abstract DeepCopyObject deepCopy();
	
	public abstract int compareObject(DeepCopyObject object);
	
	@Override
	public int compareTo(DeepCopyObject object) {
		
		return this.compareObject(object);
		
	}
	
	public static void main(String [] args) {
		
		DeepCopyObject node1 = new IntegerNode(5);
		DeepCopyObject node2 = new IntegerNode(10);
		System.out.println("result for node1 compares to node2 is: " + node1.compareTo(node2));
		
	}


}
