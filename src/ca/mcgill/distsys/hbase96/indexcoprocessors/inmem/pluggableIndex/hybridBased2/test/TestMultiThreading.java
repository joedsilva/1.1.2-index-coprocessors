package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.IMBLTree;

public class TestMultiThreading {

	// In order to make this work, remember to comment all the ByteArray section
	// in
	// IMBLTree.java

	private static class insertThread implements Runnable {

		int threadNumber, numberThread;
		IMBLTree tree;

		insertThread(int threadNumber, int numberThread, IMBLTree tree) {
			this.threadNumber = threadNumber;
			this.numberThread = numberThread;
			this.tree = tree;
		}

		@Override
		public void run() {
			int times = TestMultiThreading.count / numberThread;
			int insertNumber;
			for (int i = 0; i < times; i++) {

				insertNumber = TestMultiThreading.intArray[i * numberThread
						+ threadNumber];
				tree.put(new IntegerNode(insertNumber), new StringNode("I am "
						+ (insertNumber)));
				// System.out.println("inserting value " + (i*10 + increment));
			}
		}

	}

	static int count = 1000000;
	static int maxValue = count;
	static long startTime, endTime;
	static int[] intArray = new int[count];

	public static void main(String[] args) throws Exception {
		int numThread = 0;
		Random generator = new Random();
		for (int i = 0; i < count; i++) {
			intArray[i] = generator.nextInt(maxValue);
			// System.out.println(intArray[i]);
		}

		System.out.println("starting test....");
		System.out.println("Inserting the value...");

		IMBLTree treeMap;
		List<Thread> threads = new ArrayList<Thread>();

		
		
		
		numThread = 1;
		treeMap = new IMBLTree(6, IMBLTree.COMPARABLE_COMPARATOR);
		threads.clear();
		System.out.println("\nStarting " + numThread
				+ " threads to inserting records ranging from 0 to " + count
				+ " into the IMBLTree...");
		startTime = System.currentTimeMillis();
		for (int i = 0; i < numThread; i++) {
			Thread thread = new Thread(new insertThread(i, numThread, treeMap));
			System.out.println("starting thread " + i);
			threads.add(thread);
			thread.start();
		}
		// wait until all the threads finished
		for (int i = 0; i < numThread; i++) {
			threads.get(i).join();
		}
		endTime = System.currentTimeMillis();
		System.out.println("Time of insertion: " + (endTime - startTime)
				+ " millionSeconds");
		
		
		
		
		
		numThread = 10;
		treeMap = new IMBLTree(6, IMBLTree.COMPARABLE_COMPARATOR);
		threads.clear();
		System.out.println("\nStarting " + numThread
				+ " threads to inserting records ranging from 0 to " + count
				+ " into the IMBLTree...");
		startTime = System.currentTimeMillis();
		for (int i = 0; i < numThread; i++) {
			Thread thread = new Thread(new insertThread(i, numThread, treeMap));
			System.out.println("starting thread " + i);
			threads.add(thread);
			thread.start();
		}
		// wait until all the threads finished
		for (int i = 0; i < numThread; i++) {
			threads.get(i).join();
		}
		endTime = System.currentTimeMillis();
		System.out.println("Time of insertion: " + (endTime - startTime)
				+ " millionSeconds");

//		System.out.println("Chcking the results...");
//		// checking the inserted results
//		for (int i = 0; i < count; i++) {
//			DeepCopyObject sample = new StringNode("I am "
//					+ TestMultiThreading.intArray[i]);
//			if (IMBLTree.COMPARABLE_COMPARATOR.compare(treeMap
//					.get(new IntegerNode(TestMultiThreading.intArray[i])),
//					sample) != 0) {
//				throw new Exception("Can't find the node with key " + i);
//			}
//		}
//		System.out.println("No exception occur");

	}
}
