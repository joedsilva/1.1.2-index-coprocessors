package ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.hybridMultiThreadBased;
import ca.mcgill.distsys.hbase96.indexcoprocessorsinmem.pluggableIndex.hybridMultiThreadBased.IMBLTree.BNode;;

public interface IMBLTNodeContentWrapper {
	
	public boolean isLeaf();
	 
    public DeepCopyObject[] keys();
     
    public DeepCopyObject[] vals();
 
    public DeepCopyObject highKey();
 
    public BNode[] child();

    public BNode next();
    
    public IMBLTNodeContentWrapper getNodeContentDeepCopy();

}
