package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.test;

import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.DeepCopyObject;




public class StringNode extends DeepCopyObject {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3759066436280452554L;
	String val;
	String trick;
	
	public StringNode (String val) {
		this.val = val;
		trick = "";
	}
	
	@Override
	public DeepCopyObject deepCopy() {
		return new StringNode(val);
	}
	
	public void doTrick() {
		trick = " I am trick";
	}
	
	public String toString() {
		return ("my value is " + this.val + this.trick);
	}
	
	@Override
	public int compareObject(DeepCopyObject object) {
		return this.val.compareTo(((StringNode) object).val);
	}
	
	
	public static void main(String [] args) {
		
		StringNode node1 = new StringNode("5");
		StringNode node2 = new StringNode("10");
		System.out.println("result for node1 compares to node2 is: " + node1.compareObject(node2));

	}

	

}
