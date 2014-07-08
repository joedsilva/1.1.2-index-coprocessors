package ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex.test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.ByteArrayCriterion;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Column;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.IndexedColumnQuery;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.proto.Criterion.CompareType;
import ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex.IndexOperations;
import ca.mcgill.distsys.hbase96.indexcommonsinmem.SecondaryIndexConstants;

// Table name: example
// Column family: cf
// Qualifier: A
public class JustForTest {

	IndexOperations index;
	Configuration config;
	byte [] family, qualifier;
	String indexType;
	Object[] arguments;
	
	public static void main(String[] args) {
		System.out.println("Hello World");
		
		JustForTest test = new JustForTest();
		test.setBackground();
		test.index.createIndex(test.family, test.qualifier, null, test.indexType, test.arguments);
		test.index.printIndex(test.family, test.qualifier);
		test.index.preput(test.family, test.qualifier, Bytes.toBytes("1"), Bytes.toBytes("changed"));
		test.index.preput(test.family, test.qualifier, Bytes.toBytes("5"), Bytes.toBytes("changed"));
		test.index.preDelete(test.family, test.qualifier, Bytes.toBytes("5"), Bytes.toBytes("Cong Yu"));
		test.index.preDelete(test.family, test.qualifier, Bytes.toBytes("2"), Bytes.toBytes("Cong Yu"));
		test.index.printIndex(test.family, test.qualifier);
		
		IndexedColumnQuery query = new IndexedColumnQuery();
        query.setMustPassAllCriteria(true);
//        ByteArrayCriterion criterion = new ByteArrayCriterion(Bytes.toBytes("changed"));
//        criterion.setCompareColumn(new Column(Bytes.toBytes("cf")).setQualifier(Bytes.toBytes("A")));
//        criterion.setComparisonType(CompareType.EQUAL);
//        query.addCriterion(criterion);
//        Set<byte[]> results = test.index.execIndexedQuery(query);
//        for(byte [] result: results){
//        	System.out.println("result: " + new String (result));
//        }
        
        ByteArrayCriterion criterion = new ByteArrayCriterion(Bytes.toBytes("ab"));
        criterion.setCompareColumn(new Column(Bytes.toBytes("cf"), Bytes.toBytes("A")));
        criterion.setComparisonType(CompareType.GREATER);
        query.addCriterion(criterion);
        ByteArrayCriterion criterion1 = new ByteArrayCriterion(Bytes.toBytes("ac"));
        criterion1.setCompareColumn(new Column(Bytes.toBytes("cf"), Bytes.toBytes("A")));
        criterion1.setComparisonType(CompareType.LESS);
        query.addCriterion(criterion1);
        Set<byte[]> results = test.index.execIndexedQuery(query);
        for(byte [] result: results){
        	System.out.println("result: " + new String (result));
        }
		


	}
	
	public void setBackground() {
		String tableName = "example";
		this.family = Bytes.toBytes("cf");
		this.qualifier = Bytes.toBytes("A");
		this.config = HBaseConfiguration.create();
//		this.arguments = new Object[4];
//		this.indexType = "ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex.RegionColumnIndex";
//		
//		int maxTreeSize = this.config.getInt(SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
//                SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);

//		arguments[0] = maxTreeSize;
//		arguments[1] = this.family;
//		arguments[2] = this.qualifier;
//		arguments[3] = tableName;
		this.indexType = "ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex.HybridIndex";
		this.arguments = new Object[3];
		arguments[0] = this.family;
		arguments[1] = this.qualifier;
		arguments[2] = tableName;
		index = new IndexOperations(tableName, this.config);
		
	}

}
