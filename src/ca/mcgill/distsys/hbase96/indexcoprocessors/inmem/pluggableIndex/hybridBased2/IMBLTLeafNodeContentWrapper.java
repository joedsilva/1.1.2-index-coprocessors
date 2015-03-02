package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;

import java.util.Arrays;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.IMBLTree.BNode;


public class IMBLTLeafNodeContentWrapper implements IMBLTNodeContentWrapper{

	DeepCopyObject [] keys;
	DeepCopyObject [] vals;
	BNode next;

	public IMBLTLeafNodeContentWrapper(DeepCopyObject[] keys, DeepCopyObject[] vals,
			BNode next) {
		this.keys = keys;
		this.vals = vals;
		this.next = next;
	}
	
    public boolean isLeaf() { return true;}

    public DeepCopyObject [] keys() { return keys;}
    
    public DeepCopyObject [] vals() { return vals;}

    public DeepCopyObject highKey() {return keys[keys.length-1];}

    public BNode[] child() { return null;}
    
    public BNode next() {return next;}

    public String toString(){
        return "Leaf(K"+Arrays.toString(keys)+", V"+Arrays.toString(vals)+", L="+next+")";
    }

	@Override
	public IMBLTNodeContentWrapper getNodeContentDeepCopy() {
		DeepCopyObject[] deepCopyKeys = new DeepCopyObject[keys.length];
		for(int i = 0; i < keys.length; i++){
			if(keys[i] != null) {
				deepCopyKeys[i] = keys[i].deepCopy();
			} else {
				deepCopyKeys[i] = null;
			}
			
		}
//		DeepCopyObject[] deepCopyValues = new DeepCopyObject[vals.length];
//		for(int i = 0; i < vals.length; i++){
//			if(vals[i] != null) {
//				deepCopyValues[i] = vals[i].deepCopy();
//			} else {
//				deepCopyValues[i] = null;
//			}		
//		}
		
		BNode nextCopy = next;
		return new IMBLTLeafNodeContentWrapper(deepCopyKeys, vals, nextCopy);
	}

}
