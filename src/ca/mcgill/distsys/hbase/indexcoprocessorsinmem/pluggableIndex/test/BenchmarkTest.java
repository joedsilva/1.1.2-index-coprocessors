package ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex.test;

//import common.FileReader;

public class BenchmarkTest {
/*
	IndexOperations index;
	Configuration config;
	byte[] family, qualifier;
	String indexType;
	Object[] arguments;

	public void createHashBasedIndex() {
		String tableName = "URLs";
		this.family = Bytes.toBytes("cf");
		this.qualifier = Bytes.toBytes("startTime");
		this.config = HBaseConfiguration.create();
		this.arguments = new Object[4];
		this.indexType = "ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex.RegionColumnIndex";

		int maxTreeSize = this.config.getInt(
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE,
				SecondaryIndexConstants.PRIMARYKEY_TREE_MAX_SIZE_DEFAULT);

		arguments[0] = maxTreeSize;
		arguments[1] = this.family;
		arguments[2] = this.qualifier;
		arguments[3] = tableName;
		index = new IndexOperations(tableName, this.config);
		index.createIndex(family, qualifier, null,
				indexType, arguments);
	}

	public void createHybridBasedIndex() {
		String tableName = "URLs";
		this.family = Bytes.toBytes("cf");
		this.qualifier = Bytes.toBytes("startTime");
		this.config = HBaseConfiguration.create();
		this.indexType = "ca.mcgill.distsys.hbase.indexcoprocessorsinmem.pluggableIndex.HybridIndex";
		this.arguments = new Object[3];
		arguments[0] = this.family;
		arguments[1] = this.qualifier;
		arguments[2] = tableName;
		index = new IndexOperations(tableName, this.config);
		index.createIndex(family, qualifier, null,
				indexType, arguments);
	}

	public void testHashBasedCreatingIndex() {
		BenchmarkTest test = new BenchmarkTest();
		test.createHashBasedIndex();
		test.index.createIndex(test.family, test.qualifier, null,
				test.indexType, test.arguments);
	}

	public void testHybridBasedCreatingIndex() {
		BenchmarkTest test = new BenchmarkTest();
		test.createHybridBasedIndex();
		test.index.createIndex(test.family, test.qualifier, null,
				test.indexType, test.arguments);
	}

	public void testHashBasedLookup(String value) {

		IndexedColumnQuery query = new IndexedColumnQuery();
        query.setMustPassAllCriteria(true);
        ByteArrayCriterion criterion = new ByteArrayCriterion(Bytes.toBytes(value));
        criterion.setCompareColumn(new Column(family).setQualifier(qualifier));
        criterion.setComparisonType(CompareType.EQUAL);
        query.addCriterion(criterion);
        this.index.execIndexedQuery(query);
        
	}

	public void testHybridBasedLookup( String value) {

		IndexedColumnQuery query = new IndexedColumnQuery();
        query.setMustPassAllCriteria(true);
        ByteArrayCriterion criterion = new ByteArrayCriterion(Bytes.toBytes(value));
        criterion.setCompareColumn(new Column(family).setQualifier(qualifier));
        criterion.setComparisonType(CompareType.EQUAL);
        query.addCriterion(criterion);
        this.index.execIndexedQuery(query);
        
	}

	public void testHashBasedInsertion(String key, String value) {

		index.preput(this.family, this.qualifier, Bytes.toBytes(key), Bytes.toBytes(value));
	}

	public void testHybridBasedInsertion(String key, String value) {
		index.preput(this.family, this.qualifier, Bytes.toBytes(key), Bytes.toBytes(value));
	}

	public void testHashBasedDeletion(String key, String value) {
		index.preDelete(this.family, this.qualifier, Bytes.toBytes(key), Bytes.toBytes(value));
	}

	public void testHybridBasedDeletion(String key, String value) {
		index.preDelete(this.family, this.qualifier, Bytes.toBytes(key), Bytes.toBytes(value));
	}

	public static void main(String[] args) throws Exception {
		// create the index for table "URLs" on cf:startTime
		
		BenchmarkTest test = new BenchmarkTest();
		test.createHashBasedIndex();
		
		
		

		// read data from file
		FileReader fr = new FileReader();
		Queue<String[]> queue = fr.readFile("/home/kestefanie/data/log");
		Object[] array = (Object[]) queue.toArray();
		int arrayLength = array.length;
		String value = "";
		String key = "";
		int next;
		Random randomGenerator = new Random();
		
		

		long start, end;
		for (int i = 0; i < 12; i++) {

			next = randomGenerator.nextInt(arrayLength);
			// get the URL
			value = ((String[]) array[next])[1];
			key = ((String[]) array[next])[3].split(" ")[2];

			start = System.nanoTime();

			test.testHybridBasedInsertion(key, value + i);
//			test.testHybridBasedDeletion(key, value);
			

			end = System.nanoTime();
			System.out.print((end - start) + " ");

		}

	}
*/
}
