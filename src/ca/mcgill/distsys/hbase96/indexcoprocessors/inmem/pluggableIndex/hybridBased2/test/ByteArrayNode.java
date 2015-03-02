package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.test;


import java.util.Arrays;

import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.DeepCopyObject;


public class ByteArrayNode extends DeepCopyObject {

	byte [] val;
	
	public ByteArrayNode(byte [] val) {
		this.val = val;
	}
	@Override
	public DeepCopyObject deepCopy() {
		byte [] copyVal = new byte [val.length];
		System.arraycopy(val, 0, copyVal, 0, val.length);
		return new ByteArrayNode(copyVal);
	}
	
	public String toString() {
		return Arrays.toString(val);
	}
	
	@Override
	public int compareObject(DeepCopyObject object) {
		// because there is currently no java native byte array comparator
		return 0;
	}
	
	public static void main(String [] args) {
		
		System.out.println("test ByteArrayNode...");
		byte [] val = "this is just for test".getBytes();
		ByteArrayNode node = new ByteArrayNode(val);
		ByteArrayNode copyNode = (ByteArrayNode) node.deepCopy();
		System.out.println("old node: " + Arrays.toString(node.val));
		System.out.println("new node: " + Arrays.toString(copyNode.val));
		copyNode.val[0] = (byte) 0xba;
		System.out.println("after change -> old node: " + Arrays.toString(node.val));
		System.out.println("after change -> new node: " + Arrays.toString(copyNode.val));
		System.out.println("ByteArrayNode test finished...");
		
	}
	
	
	
	
	
}
