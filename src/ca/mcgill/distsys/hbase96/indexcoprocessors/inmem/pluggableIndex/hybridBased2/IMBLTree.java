package ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.test.IntegerNode;
import ca.mcgill.distsys.hbase96.indexcoprocessors.inmem.pluggableIndex.hybridBased2.test.StringNode;

public class IMBLTree implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1577995491716001847L;

	/** common interface for BTree node */
	public interface BNode {
		boolean isLeaf();

		Object[] keys();

		Object[] vals();

		Object highKey();

		long[] child();

		long next();

		IMBLTNodeContentWrapper getNodeContent();

		IMBLTNodeContentWrapper getNodeContentDeepCopy();

		void releaseLock();

		void setNodeContentWrapper(IMBLTNodeContentWrapper newRef);
	}

	public class DirNode implements BNode {
		final Object[] keys;
		final long[] child;
		IMBLTNodeContentWrapper nodeContentWrapper;
		private final ReentrantLock lock = new ReentrantLock();

		DirNode() {
			keys = null;
			child = null;
		}

		DirNode(Object[] keys, long[] child) {
			this.keys = keys;
			this.child = child;
		}

		DirNode(Object[] keys, List<Long> child) {
			this.keys = keys;
			this.child = new long[child.size()];
			for (int i = 0; i < child.size(); i++) {
				this.child[i] = child.get(i);
			}
		}

		@Override
		public boolean isLeaf() {
			return false;
		}

		@Override
		public Object[] keys() {
			return keys;
		}

		@Override
		public Object[] vals() {
			return null;
		}

		@Override
		public Object highKey() {
			return keys[keys.length - 1];
		}

		@Override
		public long[] child() {
			return child;
		}

		@Override
		public long next() {
			return child[child.length - 1];
		}

		@Override
		public String toString() {
			return "Dir(K" + Arrays.toString(keys) + ", C"
					+ Arrays.toString(child) + ")";
		}

		@Override
		public IMBLTNodeContentWrapper getNodeContent() {

			return nodeContentWrapper;
		}

		@Override
		public IMBLTNodeContentWrapper getNodeContentDeepCopy() {
			lock.lock();
			return nodeContentWrapper.getNodeContentDeepCopy();
		}

		@Override
		public void releaseLock() {
			lock.unlock();
		}

		@Override
		public void setNodeContentWrapper(IMBLTNodeContentWrapper newRef) {
			nodeContentWrapper = newRef;

		}

	}

	public class LeafNode implements BNode {
		final Object[] keys;
		final Object[] vals;
		final long next;
		private final ReentrantLock lock = new ReentrantLock();

		IMBLTNodeContentWrapper nodeContentWrapper;

		LeafNode() {
			keys = null;
			vals = null;
			next = 0;
		}

		LeafNode(Object[] keys, Object[] vals, long next) {
			this.keys = keys;
			this.vals = vals;
			this.next = next;
			assert (vals == null || keys.length == vals.length + 2);
		}

		@Override
		public boolean isLeaf() {
			return true;
		}

		@Override
		public Object[] keys() {
			return keys;
		}

		@Override
		public Object[] vals() {
			return vals;
		}

		@Override
		public Object highKey() {
			return keys[keys.length - 1];
		}

		@Override
		public long[] child() {
			return null;
		}

		@Override
		public long next() {
			return next;
		}

		@Override
		public String toString() {
			return "Leaf(K" + Arrays.toString(keys) + ", V"
					+ Arrays.toString(vals) + ", L=" + next + ")";
		}

		// added
		@Override
		public IMBLTNodeContentWrapper getNodeContent() {
			return nodeContentWrapper;
		}

		@Override
		public IMBLTNodeContentWrapper getNodeContentDeepCopy() {
			lock.lock();
			return nodeContentWrapper.getNodeContentDeepCopy();
		}

		@Override
		public void releaseLock() {
			lock.unlock();
		}

		@Override
		public void setNodeContentWrapper(IMBLTNodeContentWrapper newRef) {
			nodeContentWrapper = newRef;

		}
	}

	@SuppressWarnings("rawtypes")
	public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>() {
		@Override
		public int compare(Comparable o1, Comparable o2) {
			return o1.compareTo(o2);
		}
	};

	BNode rootNode;
	int maxNodeSize;
	Comparator<DeepCopyObject> comparator;
	List<BNode> leftEdges;

	public IMBLTree(int maxNodeSize, Comparator<DeepCopyObject> comparator) {

		final LeafNode emptyRoot = new LeafNode(new Object[] { null, null },
				new Object[] {}, 0);
		this.rootNode = emptyRoot;
		IMBLTLeafNodeContentWrapper rootWrapper = new IMBLTLeafNodeContentWrapper(
				new DeepCopyObject[] { null, null }, new DeepCopyObject[] {},
				null);
		this.rootNode.setNodeContentWrapper(rootWrapper);
		this.maxNodeSize = maxNodeSize;
		this.comparator = comparator;
		this.leftEdges = new ArrayList<BNode>();
		this.leftEdges.add(rootNode);
	}

	/**
	 * Find the first children node with a key equal or greater than the given
	 * key. If all items are smaller it returns `keys.length`
	 */
	protected final int findChildren(final DeepCopyObject key,
			final DeepCopyObject[] keys) {
		int left = 0;
		if (keys[0] == null)
			left++;
		int right = keys[keys.length - 1] == null ? keys.length - 1
				: keys.length;

		int middle;

		// binary search
		while (true) {
			middle = (left + right) / 2;
			if (keys[middle] == null)
				return middle; // null is positive infinitive
			if (comparator.compare(keys[middle], key) < 0) {
				left = middle + 1;
			} else {
				right = middle;
			}
			if (left >= right) {
				return right;
			}
		}

	}

	protected BNode nextDir(IMBLTInnerNodeContentWrapper d, DeepCopyObject key) {
		int pos = findChildren(key, d.keys) - 1;
		if (pos < 0)
			pos = 0;
		return d.child[pos];
	}

	/**
	 * expand array size by 1, and put value at given position. No items from
	 * original array are lost
	 */
	protected static Object[] arrayPut(final Object[] array, final int pos,
			final Object value) {
		final Object[] ret = Arrays.copyOf(array, array.length + 1);
		if (pos < array.length) {
			// copy the rest of the array
			System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
		}
		// modify the value in the pos
		ret[pos] = value;
		return ret;
	}

	// get method
	public DeepCopyObject get(DeepCopyObject key) {
		return (DeepCopyObject) get(key, true);
	}

	protected DeepCopyObject get(DeepCopyObject key, boolean expandValue) {
		if (key == null)
			throw new NullPointerException();
		DeepCopyObject v = (DeepCopyObject) key;

		BNode current = rootNode;
		IMBLTNodeContentWrapper A = current.getNodeContent();

		// dive until leaf
		while (!A.isLeaf()) {
			current = nextDir((IMBLTInnerNodeContentWrapper) A, v);
			A = current.getNodeContent();
		}

		// now at leaf level
		IMBLTLeafNodeContentWrapper leaf = (IMBLTLeafNodeContentWrapper) A;
		int pos = findChildren(v, leaf.keys);
		while (pos == leaf.keys.length) {
			// follow next link on leaf until necessary
			leaf = (IMBLTLeafNodeContentWrapper) leaf.next().getNodeContent();
			pos = findChildren(v, leaf.keys);
		}
		if (pos == leaf.keys.length - 1) {
			return null; // last key is always deleted
		}
		// finish search
		// should compare the value as well
		if (leaf.keys[pos] != null
				&& 0 == comparator.compare(v, leaf.keys[pos])) {
			Object ret = leaf.vals[pos - 1];
			// return expandValue ? valExpand(ret) : ret;
			return ((DeepCopyObject) ret);
		} else
			return null;
	}

	// put method

	public DeepCopyObject put(DeepCopyObject key, DeepCopyObject value) {
		if (key == null || value == null)
			throw new NullPointerException();
		return put(key, value, false);
	}

	protected DeepCopyObject put(DeepCopyObject key, DeepCopyObject value2,
			final boolean putOnlyIfAbsent) {

		DeepCopyObject v = key;
		if (v == null)
			throw new IllegalArgumentException("null key");
		if (value2 == null)
			throw new IllegalArgumentException("null value");

		DeepCopyObject value = value2;
		int stackPos = -1;
		BNode[] stackVals = new BNode[4];

		// This will points to the newly splitted node
		// and unlock it when the correct parent is found
		BNode previousLockedNode = null;
		BNode current = rootNode;
		IMBLTNodeContentWrapper A = current.getNodeContent();
		// Proceed until a leaf node is found
		while (!A.isLeaf()) {
			BNode t = current;
			current = nextDir((IMBLTInnerNodeContentWrapper) A, v);
			if (current == A.child()[A.child().length - 1]) {
				// is link, do nothing
				// goes to top of the while loop
			} else {
				// stack push t
				stackPos++;
				if (stackVals.length == stackPos) { // grow if needed
					// Arrays.copyOf just copy the reference
					// because we just need the copy of reference of BNode
					// then we don't need to implement deepcopy method
					stackVals = Arrays.copyOf(stackVals, stackVals.length * 2);
				}
				// push t
				stackVals[stackPos] = t;
			}
			A = current.getNodeContent();
		}

		// leaf node has been found
		int level = 1;

		// long p=0;
		BNode p = null;
		try {
			while (true) {
				boolean found;
				do {
					// make a deep copy of the contentWrapper
					// and lock the node
					A = current.getNodeContentDeepCopy();
					found = true;
					int pos = findChildren(v, A.keys());
					// check if keys is already in tree
					if (pos < A.keys().length - 1 && v != null
							&& A.keys()[pos] != null
							&& 0 == comparator.compare(v, A.keys()[pos])) {
						// We don't need to check previousLockedNode
						// because it's leaf node
						// yes key is already in tree
						Object oldVal = A.vals()[pos - 1];
						//System.out.println("IMBLTree.put -> this is invalid");
						A.vals()[pos - 1] = value;

						// because A is deep copy, don't need to create new
						// object
						current.setNodeContentWrapper(A);
						current.releaseLock();
						return ((DeepCopyObject) oldVal);
					}

					// if v > highvalue(a)
					if (A.highKey() != null
							&& comparator.compare(v, A.highKey()) > 0) {
						// follow link until necessary
						// unlock(nodeLocks, current);
						current.releaseLock();
						found = false;
						int pos2 = findChildren(v, A.keys());
						while (A != null && pos2 == A.keys().length) {
							// TODO lock?
							BNode next = A.next();

							// if(next==0) break; ==> ?? what to do with this?
							if (next == null)
								break;
							current = next;
							// A = engine.get(current, nodeSerializer);
							A = current.getNodeContent();
							pos2 = findChildren(v, A.keys());
						}

					}

				} while (!found);

				// Check if there is previousLockedNode
				// if it's locked, unlock the node
				if (previousLockedNode != null) {

					previousLockedNode.releaseLock();

				}

				// case 2: key doesn't exist, need to add key as well as value
				// most of the operation happens here
				// this is because the value will be changed by hashtable, not
				// blink tree
				// can be new item inserted into A without splitting it?
				// leafNode: minus (high key + first null value)
				// innerNode: minus (high key)
				if (A.keys().length - (A.isLeaf() ? 2 : 1) < maxNodeSize) {
					// case 2.1: node is safe => no need to split
					// all the locks can be released here
					int pos = findChildren(v, A.keys());
					Object[] keys = arrayPut(A.keys(), pos, v);

					if (A.isLeaf()) {
						// the first item in the leaf node is null
						// so we need to make the position as pos - 1
						Object[] vals = arrayPut(A.vals(), pos - 1, value);
						((ByteArrayNodeValue) value).setUpdateDone();
						IMBLTLeafNodeContentWrapper n = new IMBLTLeafNodeContentWrapper(
								(DeepCopyObject[]) keys,
								(DeepCopyObject[]) vals,
								((IMBLTLeafNodeContentWrapper) A).next);
						current.setNodeContentWrapper(n);
					} else {
						Object[] child = arrayPut(A.child(), pos, p);
						IMBLTInnerNodeContentWrapper d = new IMBLTInnerNodeContentWrapper(
								(DeepCopyObject[]) keys, (BNode[]) child);
						current.setNodeContentWrapper(d);
					}

					current.releaseLock();
					return null;
				} else {
					// case 2.2: node is not safe, it requires splitting
					final int pos = findChildren(v, A.keys());
					final Object[] keys = arrayPut(A.keys(), pos, v);
					// vals is for leaf node
					final Object[] vals = (A.isLeaf()) ? arrayPut(A.vals(),
							pos - 1, value) : null;
					((ByteArrayNodeValue) value).setUpdateDone();
					// final long[] child = A.isLeaf()? null :
					// child is for inner node
					final Object[] child = A.isLeaf() ? null : arrayPut(
							A.child(), pos, p);
					final int splitPos = keys.length / 2;
					BNode B;
					// update the newly created node
					if (A.isLeaf()) {
						Object[] vals2 = Arrays.copyOfRange(vals, splitPos,
								vals.length);
						// create newly splitted leaf node
						// the first key of the leaf node should be null ???
						IMBLTLeafNodeContentWrapper wrapper1 = new IMBLTLeafNodeContentWrapper(
								((DeepCopyObject[]) Arrays.copyOfRange(keys,
										splitPos, keys.length)),
								((DeepCopyObject[]) vals2),
								((IMBLTLeafNodeContentWrapper) A).next);

						B = new LeafNode();
						B.setNodeContentWrapper(wrapper1);

					} else {
						// create newly splitted dir node
						IMBLTInnerNodeContentWrapper wrapper1 = new IMBLTInnerNodeContentWrapper(
								((DeepCopyObject[]) Arrays.copyOfRange(keys,
										splitPos, keys.length)),
								((BNode[]) Arrays.copyOfRange(child, splitPos,
										keys.length)));
						B = new DirNode();
						B.setNodeContentWrapper(wrapper1);
					}
					// B is the newly created node
					BNode q = B;
					if (A.isLeaf()) { // splitPos+1 is there so A gets new high
										// value (key)
						// ???
						Object[] keys2 = Arrays.copyOf(keys, splitPos + 2);
						keys2[keys2.length - 1] = keys2[keys2.length - 2];
						Object[] vals2 = Arrays.copyOf(vals, splitPos);

						A = new IMBLTLeafNodeContentWrapper(
								(DeepCopyObject[]) keys2,
								(DeepCopyObject[]) vals2, (BNode) q);
					} else {
						Object[] child2 = Arrays.copyOf(child, splitPos + 1);
						child2[splitPos] = q;

						A = new IMBLTInnerNodeContentWrapper(
								(DeepCopyObject[]) Arrays.copyOf(keys,
										splitPos + 1), (BNode[]) child2);
					}

					current.setNodeContentWrapper(A);

					// insert the high key and pointer to the parent
					if ((current != rootNode)) { // is not root
						// according to the original algorithm, if the node is
						// splitted,
						// the original node will be locked until the correct
						// correct parent
						// node is found. So we don't release the lock here
						// current.releaseLock();
						previousLockedNode = current;
						p = q;
						v = (DeepCopyObject) A.highKey();
						level = level + 1;
						if (stackPos != -1) { // if stack is not empty
							current = stackVals[stackPos--];
						} else {
							// current := the left most node at level
							current = leftEdges.get(level - 1);
						}
					} else {

						IMBLTInnerNodeContentWrapper rootWrapper = new IMBLTInnerNodeContentWrapper(
								new DeepCopyObject[] {
										A.keys()[0],
										A.highKey(),
										B.isLeaf() ? null : B.getNodeContent()
												.highKey() }, new BNode[] {
										current, q, null });
						BNode R = new DirNode();
						R.setNodeContentWrapper(rootWrapper);
						rootNode = R;
						leftEdges.add(R);
						current.releaseLock();

						return null;
					}
				}
			}
		} catch (RuntimeException e) {
			// unlockAll(nodeLocks);
			throw e;
		} catch (Exception e) {
			// unlockAll(nodeLocks);
			throw new RuntimeException(e);
		}
	}

	// remove method
	public DeepCopyObject remove(final DeepCopyObject key,
			final DeepCopyObject value) {

		BNode current = rootNode;
		IMBLTNodeContentWrapper A = current.getNodeContent();
		while (!A.isLeaf()) {
			current = nextDir((IMBLTInnerNodeContentWrapper) A, key);
			// A = engine.get(current, nodeSerializer);
			A = current.getNodeContent();
		}

		try {
			while (true) {
				// lock the node and get the deep copy of the node
				A = current.getNodeContentDeepCopy();
				int pos = findChildren(key, A.keys());
				if (pos < A.keys().length && key != null
						&& A.keys()[pos] != null
						&& 0 == comparator.compare(key, A.keys()[pos])) {
					// check for last node which was already deleted
					if (pos == A.keys().length - 1 && value == null) {
						// unlock(nodeLocks, current);
						current.releaseLock();
						return null;
					}

					// delete from node
					DeepCopyObject oldVal = A.vals()[pos - 1];
					if (value != null && comparator.compare(value, oldVal) != 0) {
						current.releaseLock();
						return null;
					}

					// there is a new valueNode with same key has been inserted!
					 if(((ByteArrayNodeValue) oldVal).getPKRefs().size() != 0)
					 {
					 current.releaseLock();
					 return null;
					 }

					DeepCopyObject[] keys2 = new DeepCopyObject[A.keys().length - 1];
					// Copy the new keys
					System.arraycopy(A.keys(), 0, keys2, 0, pos);
					System.arraycopy(A.keys(), pos + 1, keys2, pos,
							keys2.length - pos);

					DeepCopyObject[] vals2 = new DeepCopyObject[A.vals().length - 1];
					// Copy the new values
					System.arraycopy(A.vals(), 0, vals2, 0, pos - 1);
					System.arraycopy(A.vals(), pos, vals2, pos - 1,
							vals2.length - (pos - 1));

					A = new IMBLTLeafNodeContentWrapper(
							(DeepCopyObject[]) keys2, (DeepCopyObject[]) vals2,
							A.next());
					current.setNodeContentWrapper(A);
					current.releaseLock();
					return (DeepCopyObject) oldVal;
				} else {
					current.releaseLock();
					// follow link until necessary
					if (A.highKey() != null
							&& comparator.compare(key, A.highKey()) > 0) {
						int pos2 = findChildren(key, A.keys());
						while (pos2 == A.keys().length) {
							current = A.next();
							A = current.getNodeContent();

						}
					} else {
						return null;
					}
				}
			}
		} catch (RuntimeException e) {
			// unlockAll(nodeLocks);
			throw e;
		} catch (Exception e) {
			// unlockAll(nodeLocks);
			throw new RuntimeException(e);
		}
	}

	// range search method

	public void getUpperNodeSubList(DeepCopyObject upperBound,
			boolean isInclusiveUpper, DeepCopyObject[] keys,
			DeepCopyObject[] vals, List<DeepCopyObject> list) {
		int position = this.findChildren(upperBound, keys);
		// upperBound exceeds maximum value of the high key
		if (position == keys.length - 1) {
			list.addAll(Arrays.asList(vals));
			return;
		}
		if (comparator.compare(upperBound, keys[position]) >= 0) {
			// list.addAll(Arrays.asList(Arrays.))
			DeepCopyObject[] subList = Arrays.copyOf(vals, position);
			if (!isInclusiveUpper) {
				if (comparator.compare(keys[position], upperBound) == 0) {
					list.addAll(Arrays.asList(Arrays.copyOf(subList,
							position - 1)));
				}
			} else {
				list.addAll(Arrays.asList(subList));
			}
		}

	}

	// return the BNode currently copied
	public BNode getLowerNodeSublist(DeepCopyObject lowerBound,
			boolean isInclusiveLower, List<DeepCopyObject> list) {
		BNode current = rootNode;
		IMBLTNodeContentWrapper A = current.getNodeContent();

		while (!A.isLeaf()) {
			current = nextDir((IMBLTInnerNodeContentWrapper) A, lowerBound);
			A = current.getNodeContent();
		}

		IMBLTLeafNodeContentWrapper leaf = (IMBLTLeafNodeContentWrapper) A;
		int pos = findChildren(lowerBound, leaf.keys);
		while (pos == leaf.keys.length) {
			// follow next link on leaf until necessary
			leaf = (IMBLTLeafNodeContentWrapper) leaf.next().getNodeContent();
			pos = findChildren(lowerBound, leaf.keys);
		}

		if (pos == leaf.keys.length - 1) {
			// no need to add values to the list
			return current; // last key is always deleted
		}

		if (comparator.compare(lowerBound, leaf.keys[pos]) == 0) {
			if (isInclusiveLower) {
				list.addAll(Arrays.asList(Arrays.copyOfRange(A.vals(), pos - 1,
						A.vals().length)));
			} else {
				list.addAll(Arrays.asList(Arrays.copyOfRange(A.vals(), pos,
						A.vals().length)));
			}
		} else {
			if (comparator.compare(lowerBound, leaf.keys[pos]) < 0) {
				list.addAll(Arrays.asList(Arrays.copyOfRange(A.vals(), pos - 1,
						A.vals().length)));
			} else {
				list.addAll(Arrays.asList(Arrays.copyOfRange(A.vals(), pos,
						A.vals().length)));
			}
		}

		return current;

	}

	public List<DeepCopyObject> rangeSearch(DeepCopyObject lowerBound,
			boolean isInclusiveLower, DeepCopyObject upperBound,
			boolean isInclusiveUpper) {
		List<DeepCopyObject> list = new ArrayList<DeepCopyObject>();

		// case 1: invalid case
		if (((lowerBound == null) && (upperBound == null))) {
			return list;
		}

		// case 2: lowerBound == null || upperBound == null
		if (lowerBound == null || upperBound == null) {
			// case 2.1: lowerBound is null
			if (lowerBound == null) {
				BNode current = leftEdges.get(0);
				if (!current.isLeaf()) {
					return list;
				}
				IMBLTNodeContentWrapper wrapper;
				while (current != null) {
					wrapper = current.getNodeContent();
					if (wrapper.highKey() == null) {
						this.getUpperNodeSubList(upperBound, isInclusiveUpper,
								wrapper.keys(), wrapper.vals(), list);
						return list;
					}
					if (comparator.compare(upperBound, wrapper.highKey()) > 0) {
						list.addAll(Arrays.asList(wrapper.vals()));
					} else {
						this.getUpperNodeSubList(upperBound, isInclusiveUpper,
								wrapper.keys(), wrapper.vals(), list);
						return list;
					}
					current = wrapper.next();

				}

			} else { // case 2.2: upperBound is null
				BNode current = this
						.getLowerNodeSublist(lowerBound, isInclusiveLower, list)
						.getNodeContent().next();
				while (current != null) {
					list.addAll(Arrays.asList(current.getNodeContent().vals()));
					current = current.getNodeContent().next();
				}

				return list;
			}
		}

		// case 3: incoming parameter is not valid
		if (comparator.compare(lowerBound, upperBound) > 0) {
			return list;
		}

		// case 4: lowerBound == upperBound
		if (comparator.compare(lowerBound, upperBound) == 0) {
			if (isInclusiveLower || isInclusiveUpper) {
				list.add(this.get(lowerBound));
			}
			return list;
		}

		// case 4: lowerBound != null && upperBound != null && lowerBound <
		// upperBound

		BNode current = this.getLowerNodeSublist(lowerBound, isInclusiveLower,
				list);
		if ((current.getNodeContent().next() != null)
				&& (comparator.compare(upperBound, current.getNodeContent()
						.highKey()) > 0)) {
			current = current.getNodeContent().next();
			IMBLTNodeContentWrapper wrapper;
			while (current != null) {
				wrapper = current.getNodeContent();
				if (wrapper.highKey() == null) {
					this.getUpperNodeSubList(upperBound, isInclusiveUpper,
							wrapper.keys(), wrapper.vals(), list);
					return list;
				}
				if (comparator.compare(upperBound, wrapper.highKey()) > 0) {
					list.addAll(Arrays.asList(wrapper.vals()));
				} else {
					this.getUpperNodeSubList(upperBound, isInclusiveUpper,
							wrapper.keys(), wrapper.vals(), list);
					return list;
				}
				current = wrapper.next();

			}
		} else { // the same node contains upperBound and lowerBound
			list.clear();
			IMBLTNodeContentWrapper wrapper = current.getNodeContent();
			int i, j;
			for (i = 1; i < wrapper.keys().length; i++) {
				if (comparator.compare(wrapper.keys()[i], lowerBound) < 0) {
					// did nothing
				} else {
					if (comparator.compare(wrapper.keys()[i], lowerBound) == 0) {
						if (isInclusiveLower) {
							list.add(wrapper.vals()[i - 1]);
						}
						i = i + 1;
					}
					break;
				}

			}

			for (j = i; j < wrapper.keys().length; j++) {
				if (j == (wrapper.keys().length - 1)) {
					return list;
				}
				if (comparator.compare(upperBound, wrapper.keys()[j]) > 0) {
					list.add(wrapper.vals()[j - 1]);
				}
				if (comparator.compare(upperBound, wrapper.keys()[j]) == 0) {
					if (isInclusiveUpper) {
						list.add(wrapper.vals()[j - 1]);
					}
					return list;
				}
				if (comparator.compare(upperBound, wrapper.keys()[j]) < 0) {
					return list;
				}
			}

		}
		return list;

	}

	// helper method
	public void printWholeTree() {
		int counter = 1;
		BNode current;
		System.out.println("Total level: " + leftEdges.size());
		for (int i = leftEdges.size() - 1; i > 0; i--) {
			System.out.println("Level: " + counter);
			counter++;
			current = leftEdges.get(i);
			printInnerLevel(current);
		}
		System.out.println("Level: " + counter);
		current = leftEdges.get(0);
		printLeafLevel(current);
	}

	public void printInnerLevel(BNode node) {
		BNode levelNode = node;
		IMBLTNodeContentWrapper contentWrapper;
		int counter = 1;
		while (levelNode != null) {
			contentWrapper = levelNode.getNodeContent();
			System.out.println("node" + counter++ + ": keys: "
					+ Arrays.toString(contentWrapper.keys()) + " child: "
					+ Arrays.toString(contentWrapper.child()));
			levelNode = contentWrapper.next();
		}
	}

	public void printLeafLevel(BNode node) {
		BNode levelNode = node;
		IMBLTNodeContentWrapper contentWrapper;
		int counter = 1;
		while (levelNode != null) {
			contentWrapper = levelNode.getNodeContent();
			System.out.println("node" + counter++ + ": keys: "
					+ Arrays.toString(contentWrapper.keys()) + " vals: "
					+ Arrays.toString(contentWrapper.vals()));
			levelNode = contentWrapper.next();
		}
	}

	public void printLeafNodeContent() {

		BNode current = leftEdges.get(0);
		IMBLTNodeContentWrapper contentWrapper = current.getNodeContent();
		if (!contentWrapper.isLeaf()) {
			System.out.println("Something wrong.... not leaf");
		} else {
			printLeafLevel(current);
		}

	}

	public BNode getFirstLeafNode() {
		return this.leftEdges.get(0);
	}

	public static void main(String[] args) {

		System.out.println("Test for BTreeMap.....");
		IMBLTree treeMap = new IMBLTree(6, IMBLTree.COMPARABLE_COMPARATOR);
		Hashtable<DeepCopyObject, DeepCopyObject> table = new Hashtable<DeepCopyObject, DeepCopyObject>();

		IntegerNode keyNode1 = new IntegerNode(1);
		IntegerNode keyNode2 = new IntegerNode(2);
		IntegerNode keyNode3 = new IntegerNode(3);
		IntegerNode keyNode4 = new IntegerNode(4);
		IntegerNode keyNode5 = new IntegerNode(5);
		IntegerNode keyNode6 = new IntegerNode(6);
		IntegerNode keyNode7 = new IntegerNode(7);
		IntegerNode keyNode8 = new IntegerNode(8);
		IntegerNode keyNode11 = new IntegerNode(11);
		IntegerNode keyNode12 = new IntegerNode(12);
		IntegerNode keyNode13 = new IntegerNode(13);
		IntegerNode keyNode14 = new IntegerNode(14);
		IntegerNode keyNode15 = new IntegerNode(15);
		IntegerNode keyNode16 = new IntegerNode(16);
		IntegerNode keyNode17 = new IntegerNode(17);
		IntegerNode keyNode18 = new IntegerNode(18);

		StringNode valueNode1 = new StringNode("1");
		StringNode valueNode2 = new StringNode("2");
		StringNode valueNode3 = new StringNode("3");
		StringNode valueNode4 = new StringNode("4");
		StringNode valueNode5 = new StringNode("5");
		StringNode valueNode6 = new StringNode("6");
		StringNode valueNode7 = new StringNode("7");
		StringNode valueNode8 = new StringNode("8");
		StringNode valueNode11 = new StringNode("11");
		StringNode valueNode12 = new StringNode("12");
		StringNode valueNode13 = new StringNode("13");
		StringNode valueNode14 = new StringNode("14");
		StringNode valueNode15 = new StringNode("15");
		StringNode valueNode16 = new StringNode("16");
		StringNode valueNode17 = new StringNode("17");
		StringNode valueNode18 = new StringNode("18");

		// put, remove, get
		treeMap.put(keyNode1, valueNode1);
		treeMap.put(keyNode2, valueNode2);
		treeMap.put(keyNode3, valueNode3);
		treeMap.put(keyNode4, valueNode4);
		treeMap.put(keyNode5, valueNode5);
		treeMap.put(keyNode6, valueNode6);
		treeMap.put(keyNode7, valueNode7);
		treeMap.put(keyNode8, valueNode8);
		treeMap.put(keyNode11, valueNode11);
		treeMap.put(keyNode12, valueNode12);
		treeMap.put(keyNode13, valueNode13);
		treeMap.put(keyNode14, valueNode14);
		treeMap.put(keyNode15, valueNode15);
		treeMap.put(keyNode16, valueNode16);
		treeMap.put(keyNode17, valueNode17);
		treeMap.put(keyNode18, valueNode18);
		table.put(keyNode1, valueNode1);
		table.put(keyNode2, valueNode2);
		table.put(keyNode3, valueNode3);
		table.put(keyNode4, valueNode4);
		table.put(keyNode5, valueNode5);
		table.put(keyNode6, valueNode6);
		table.put(keyNode7, valueNode7);
		table.put(keyNode8, valueNode8);
		table.put(keyNode11, valueNode11);
		table.put(keyNode12, valueNode12);
		table.put(keyNode13, valueNode13);
		table.put(keyNode14, valueNode14);
		table.put(keyNode15, valueNode15);
		table.put(keyNode16, valueNode16);
		table.put(keyNode17, valueNode17);
		table.put(keyNode18, valueNode18);

		System.out.println(treeMap.get(new IntegerNode(1)));
		System.out.println(treeMap.get(new IntegerNode(2)));
		System.out.println(treeMap.get(new IntegerNode(3)));
		System.out.println(treeMap.get(new IntegerNode(4)));
		System.out.println(treeMap.get(new IntegerNode(5)));
		System.out.println(treeMap.get(new IntegerNode(6)));
		System.out.println(treeMap.get(new IntegerNode(7)));
		System.out.println(treeMap.get(new IntegerNode(9)));

		// System.out.println("removig......");
		// System.out.println(treeMap.remove(new IntegerNode(1), new
		// StringNode("1")));
		// System.out.println(treeMap.get(new IntegerNode(1)));
		// treeMap.remove(new IntegerNode(1), new StringNode("1"));
		// treeMap.remove(new IntegerNode(2), new StringNode("2"));
		// treeMap.remove(new IntegerNode(3), new StringNode("3"));
		// treeMap.remove(new IntegerNode(4), new StringNode("4"));
		// treeMap.printLeafNodeContent();
		treeMap.printWholeTree();

		// rangeSearch test
		System.out.println("rangeSearch...");
		System.out.println(Arrays.toString(treeMap.rangeSearch(
				new IntegerNode(8), true, new IntegerNode(8), true).toArray()));
		System.out.println(Arrays.toString(treeMap.rangeSearch(null, true,
				new IntegerNode(18), false).toArray()));
		System.out.println(Arrays.toString(treeMap.rangeSearch(
				new IntegerNode(19), true, null, false).toArray()));
		System.out.println(Arrays
				.toString(treeMap.rangeSearch(new IntegerNode(2), true,
						new IntegerNode(17), false).toArray()));
		System.out.println(Arrays
				.toString(treeMap.rangeSearch(new IntegerNode(17), true,
						new IntegerNode(18), true).toArray()));

		// test doTrick method
		System.out.println("Hello :)");
		StringNode strNode = (StringNode) table.get(keyNode1);
		strNode.doTrick();
		System.out.println((treeMap.get(keyNode1) == valueNode1));
		treeMap.printLeafNodeContent();

	}
}
