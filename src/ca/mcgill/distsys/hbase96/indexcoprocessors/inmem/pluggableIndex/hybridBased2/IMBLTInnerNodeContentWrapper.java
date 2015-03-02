package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;

import java.util.Arrays;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.IMBLTree.BNode;

public class IMBLTInnerNodeContentWrapper implements IMBLTNodeContentWrapper{

	DeepCopyObject[] keys;
    BNode[] child;
    
    public IMBLTInnerNodeContentWrapper(DeepCopyObject[] keys, BNode[] child) {
        this.keys = keys;
        this.child = child;
    }
    
    public boolean isLeaf() { return false;}
 
    public DeepCopyObject[] keys() { return keys;}
     
    public DeepCopyObject[] vals() { return null;}
 
    public DeepCopyObject highKey() {return keys[keys.length-1];}
 
    public BNode[] child() { return child;}

    // link pointer to the sibling
    public BNode next() {return child[child.length-1];}
 
    public String toString(){
        return "Dir(K"+Arrays.toString(keys)+", C"+Arrays.toString(child)+")";
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
		BNode[] childCopy = new BNode [child.length];
		System.arraycopy(child, 0, childCopy, 0, child.length);
		
		return new IMBLTInnerNodeContentWrapper(deepCopyKeys, childCopy);
	}

}
