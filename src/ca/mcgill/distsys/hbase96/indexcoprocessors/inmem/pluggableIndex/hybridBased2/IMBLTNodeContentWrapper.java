package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.IMBLTree.BNode;

public interface IMBLTNodeContentWrapper {
	
	public boolean isLeaf();
	 
    public DeepCopyObject[] keys();
     
    public DeepCopyObject[] vals();
 
    public DeepCopyObject highKey();
 
    public BNode[] child();

    public BNode next();
    
    public IMBLTNodeContentWrapper getNodeContentDeepCopy();

}
