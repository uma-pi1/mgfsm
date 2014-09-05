package de.mpii.fsm.bfs;


import org.apache.mahout.math.function.IntIntProcedure;
import org.apache.mahout.math.map.OpenIntIntHashMap;

import de.mpii.fsm.driver.FsmConfig.Type;
import de.mpii.fsm.mgfsm.FsmJob;
import de.mpii.fsm.mgfsm.FsmWriter;
import de.mpii.fsm.mgfsm.FsmWriterForReducer;
import de.mpii.fsm.util.IntArrayStrategy;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;


/**
 * Computation of frequent subsequences using a breadth-first search. 
 * 
 * This class takes a set of input transactions (along with their frequencies) and computes 
 * all frequent (lambda,gamma)-subsequences with minimum support sigma, where lambda, gamma, and sigma
 * are parameters. A sequence is a (lambda,gamma)-subsequence of a transaction if it 
 * consists of at most lambda items and occurs in the transaction at positions separated 
 * by at most gamma items. The support of a (lambda,gamma)-subsequence is given by 
 * the number of transactions in which it occurs. 
 * 
 * This class can be used either to discover all frequent subsequences (standalone) 
 * or to discover so-called pivot sequences (MapReduce). A sequence is a pivot sequence 
 * with respect to a range [beginItem, endItem] if it contains at least one item in 
 * [beginItem, endItem] and no item larger than endItem (here we assume that items are 
 * ordered). Mining of pivot* sequences is used during distributed frequent sequence 
 * mining with MapReduce (see {@link FsmJob}).
 *    
 * Each input transaction is given as a sequence of integers. Positive integers correspond to
 * items (or item identifiers). Negative integers correspond to gaps; e.g., value -3 indicates
 * that there are three items that should be ignored for mining. The general contract of
 * this class is that the input transaction must not contain any items larger than endItem.
 * 
 * This class is not thread-safe.
 *
 * @author Iris Miliaraki (miliaraki@mpi-inf.mpg.de)
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 * @author Spyros Zoupanos (spyros@zoupanos.net)
 * @author Rainer Gemulla (rgemulla@mpi-inf.mpg.de)
 * @author Kaustubh Beedkar (kbeedkar@mpi-inf.mpg.de)
 */
public class BfsMiner {

  // -- parameters --------------------------------------------------------------------------------

  /** Minimum support */
  protected int sigma;

  /** Maximum gap */
  protected int gamma;

  /** Maximum length */
  protected int lambda;

  /** Start of pivot range (see class description). Set to 0 to mine all frequent sequences. */
  protected int beginItem = 0;

  /** End of pivot range (see class description). Set to {@link Integer.MAXVALUE} to mine 
   * all frequent sequences. */
  protected int endItem = Integer.MAX_VALUE;

  /** All, maximal or closed. Default is all */
  protected Type outputType = Type.ALL;

  /** Whether or not to buffer all input transactions before processing. Buffering takes some time,
   * but allows for optimizations that are particularly effective if the gap parameter is large 
   * (e.g., counting locally infrequent items and minimizing the indexing work).
   */
  protected boolean bufferTransactions = false;
  
  // -- internal variables ------------------------------------------------------------------------

  // At any point of time, we store an inverted index that maps subsequences of length k
  // to their posting lists.  
  //
  // During data input, we have k=2. Every input transaction is added to the index (by generating
  // all its (2,gamma)-subsequences and then discarded.
  //
  // During frequent sequence mining, we compute repeatedly compute length-(k+1) subsequences
  // from the length-k subsequences.

  /** Length of subsequences currently being mined */
  int k;

  /** A list of sequences of length k; no sequence occurs more than once. Each sequence is 
   * stored in either uncompressed or compressed format. 
   * 
   * In uncompressed format, each sequence is encoded as an array of exactly k item 
   * identifiers. When k=2, all sequences are stored in uncompressed format. 
   * 
   * In compressed format, each sequence is encoded as a length-2 array (p, w). To 
   * reconstruct the uncompressed sequence, take the first k-1 items from the sequence
   * at position p in kSequences (p is a "prefix pointer") and set the k-th item to w (suffix
   * item). When k>2, an entry is compressed when it has two elements and uncompressed when 
   * it has k elements.
   */
  protected ArrayList<int[]> kSequences = new ArrayList<int[]>();

  /** Maps 2-sequence to their position in kSequences (lowest 32 bits) and their largest 
   * transaction id (highest 32 bits). Only used during data input, k=2.
   */
  Map<int[], KSequenceIndexEntry> kSequenceIndex = new Object2ObjectOpenCustomHashMap<int[], KSequenceIndexEntry>(new IntArrayStrategy());

  /** Holds information about a posting list. Used only during data input, k=32. */
  private class KSequenceIndexEntry {
    int index;
    int lastTransactionId;
    int lastPosition;
  }

  /** Used as a temporary buffer during data input. */
  final int[] twoSequenceTemp = new int[2];

  /** Posting list for each sequence in kSequences. A posting list is a set of postings,
   * one for each transaction in which the sequence occurs. Every posting consists of a
   * transaction identifier and a list of starting positions (at which a match of the
   * sequence occurs in the respective transactions). Transactions and starting positions
   * within transactions are sorted in ascending order. 
   * Each posting is encoded using variable-length integer encoding; postings are separated
   * by a 0 byte. To avoid collisions, we store transactionId+1 and position+1. 
   * (Note that not every 0 byte separates posting; the byte before the 0 byte
   * must have its highest-significant bit set to 0).
   */
  //
  protected ArrayList<ByteArrayList> kPostingLists = new ArrayList<ByteArrayList>();

  /** Support of each transaction (indexed by transaction id). If an input transaction has
   * support larger than one, is it treated as if it had occured in the data as many times 
   * as given by its support value. */
  protected IntArrayList transactionSupports = new IntArrayList();

  /** Total support for each sequence in kSequences. Identical to the sum of the supports
   * of each transaction that occurs in the posting list. */
  protected IntArrayList kTotalSupports = new IntArrayList();

  /** Total support of all input sequences seen thus far. */
  protected int totalSupport = 0;

  
  /** If {@link #bufferTransactions}, storas all input transactions. */
  protected ArrayList<int[]> inputTransactions = new ArrayList<int[]>();

  /** If {@link #bufferTransactions}, stores the support of each individual item. */  
  protected OpenIntIntHashMap k1TotalSupports = new OpenIntIntHashMap();

  /** If {@link #bufferTransactions}, stores, for each item, the largest support of a 2-sequence 
   * that has this item as its first item */
  protected OpenIntIntHashMap k2MaxLeftSupport = new OpenIntIntHashMap();
  
  /** If {@link #bufferTransactions}, set of items seen in the current transaction */
  protected IntOpenHashSet itemsInTransactionTemp = new IntOpenHashSet();
  
  /** If {@link #outputType = maximal or closed}, store the support of frequent items */
  protected OpenIntIntHashMap fListMap = new OpenIntIntHashMap(); 

  // -- initialization/clearing -------------------------------------------------------------------

  /**
   * @param sigma minimum support
   * @param gamma maximum gap 
   * @param lambda maximum length
   */
  public BfsMiner(int sigma, int gamma, int lambda) {
    this.sigma = sigma;
    this.gamma = gamma;
    this.lambda = lambda;
  }

  /** Sets flag for whether or not to buffer input transactions. See {@link #bufferTransactions}
   * for details.
   * @param bufferTransactions
   */
  public void setBufferTransactions(boolean bufferTransactions) {
    this.bufferTransactions = bufferTransactions;
  }

  /** Flushes all internal data structures. */
  public void clear() {
    k = 2;
    kSequenceIndex.clear();
    kSequences.clear();
    kPostingLists.clear();
    totalSupport = 0;
    inputTransactions.clear();
    k2MaxLeftSupport.clear();
    kTotalSupports.clear();
    transactionSupports.clear();
    fListMap.clear();
  }

  /** Updates the parameters used for sequence mining and flushes internal data structures.
   *
   * @param sigma minimum support
   * @param gamma maximum gap 
   * @param lambda maximum length
   */
  public void setParametersAndClear(int sigma, int gamma, int lambda) {
    this.sigma = sigma;
    this.gamma = gamma;
    this.lambda = lambda;
    clear();
  }

  /** Updates the parameters used for sequence mining and flushes internal data structures.
  *
  * @param sigma minimum support
  * @param gamma maximum gap 
  * @param lambda maximum length
  * @param outputSequenceType maximal/closed/all
  */
  public void setParametersAndClear(int sigma, int gamma, int lambda, Type outputType) {
    this.sigma = sigma;
    this.gamma = gamma;
    this.lambda = lambda;
    this.outputType = outputType;
    clear();
  }

  /** Initialize frequent sequence miner (without pivots). Should be called before any data 
   * input. */
  public void initialize() {
    initialize(0, Integer.MAX_VALUE);
  }

  /** Initialize frequent sequence miner (with pivots). Should be called before any data 
   * input. 
   * 
   * @param beginItem begin of pivot range (see class description)
   * @param endItem end of pivot range (see class description) 
   */
  public void initialize(int beginItem, int endItem) {
    clear();
    this.beginItem = beginItem;
    this.endItem = endItem;
  }
  
  public void setFlistMap(OpenIntIntHashMap fListMap){
	  this.fListMap = fListMap;
  }

  // -- input phase -------------------------------------------------------------------------------

  /** Add an input transaction to this miner. This method must only be called in between calls to 
   * {@link BfsMiner#initialize()} and {@link BfsMiner#mineAndClear(FsmWriterForReducer)}. 
   * 
   * @param transaction input transaction (see class description for expected format)
   * @param fromIndex start of input transaction in transaction array
   * @param toIndex end of input transaction in transaction array
   * @param support support of the input transaction (i.e., how many times did it occur in the data?)  
   */
  public void addTransaction(int[] transaction, int fromIndex, int toIndex, int support) {
    // update supports
    totalSupport += support;
    transactionSupports.add(support);
    
    if (bufferTransactions) {
      // buffer transaction
      int length = toIndex - fromIndex;
      int[] inputTransaction = new int[length];
      System.arraycopy(transaction, fromIndex, inputTransaction, 0, toIndex - fromIndex);
      inputTransactions.add(inputTransaction);

      // update support of each item
      itemsInTransactionTemp.clear();
      for (int i = fromIndex; i < toIndex; i++) {
        assert transaction[i] <= endItem; // contract of this class 

        // skip gaps
        if (transaction[i] < 0) {
          continue;
        } else {
          int item = transaction[i];
          if (itemsInTransactionTemp.add(item)) { // remember that we've seen this item
            // count only first occurrence of an item within a transaction
            k1TotalSupports.adjustOrPutValue(item, support, support);
          } 
        }
      }
    } else {
      // do not buffer transactions; directly build 2-sequence index
      addTransactionToIndex(transaction, fromIndex, toIndex, support, transactionSupports.size()-1,
          Integer.MAX_VALUE);
    }
  }

  /** Add an input transaction to the index of 2-sequences.  
   * 
   * @param transaction input transaction (see class description for expected format)
   * @param fromIndex start of input transaction in transaction array
   * @param toIndex end of input transaction in transaction array
   * @param support support of the input transaction (i.e., how many times did it occur in the data?)
   * @param transactionId id of current transaction (assumed to be incremental)
   * @param remainingSupport lower bound on support of all remaining transactions; the posting list 
   *    of the 2-sequence is only created/updated if it can potentially reach support sigma
   */
  private void addTransactionToIndex(int[] transaction, int fromIndex, int toIndex, int support,
      int transactionId, int remainingSupport) {
    // only valid during data input phase
    assert k == 2;
    assert kSequences.size() == kSequenceIndex.size();

    // Add the transaction to the inverted index. Here we construct all gapped 2-sequences 
    // and update their corresponding posting lists
    int pos = 0; // current position in expanded sequence (i.e., without compressed gaps)
    for (int i = fromIndex; i < toIndex; i++) {
      assert transaction[i] <= endItem; // contract of this class 

      // skip gaps
      if (transaction[i] < 0) {
        pos -= transaction[i];
        continue;
      }

      // ignore locally infrequent 1-seqs or, when we processed most transactions, items that 
      // do not occur sufficiently frequently at the start of a 2-sequence 
      if (bufferTransactions) {
        if (k1TotalSupports.get(transaction[i]) < sigma || 
            k2MaxLeftSupport.get(transaction[i]) + remainingSupport < sigma) {
          pos++;
          continue;
        }
      }

      // create all 2-subsequences
      // i points to first item, j to second item
      int gapFromI = -1;
      for (int j = i + 1; j < toIndex; j++) {
        // compute gap of current item from item i
        if (transaction[j] < 0) {
          gapFromI -= transaction[j];
          continue;
        } else {
          gapFromI += 1;
        }

        // are we done?
        if (gapFromI > gamma) {
          break;
        }

        // ignore locally infrequent 1-seqs (right item)
        if (bufferTransactions) {
          if (k1TotalSupports.get(transaction[j]) < sigma) {
            continue;
          }
        }

        // we found a valid 2-sequence; create a posting
        twoSequenceTemp[0] = transaction[i];
        twoSequenceTemp[1] = transaction[j];
        addPosting(twoSequenceTemp, transactionId, support, pos, sigma-remainingSupport);
      }

      pos++;
    }
  }

  /** Adds an occurrence of a 2-sequence to the inverted index. Only used for 2-sequences during
   * the input phase. The provided kSequence is not stored, i.e., can be reused. 
   * 
   * @param kSequence 2-sequence to add
   * @param transactionId id of the transaction in which the 2-sequence occurs in
   * @param support support of that transaction
   * @param position (logical) position of the item in the transaction 
   * @param minSupportOfPosting the posting is only created/updated if it already has at least
   *  this support
   */
  private void addPosting(int[] kSequence, int transactionId, int support, int position,
      int minSupportOfPosting) {
    // get the posting list for the current sequence
    // if the sequence has not seen before, create a new posting list
    KSequenceIndexEntry entry = kSequenceIndex.get(kSequence);
    //TByteArrayList postingList;
    ByteArrayList postingList;
    if (entry == null) {
      // we never saw this 2-sequence before
      if (minSupportOfPosting>0) return; // nothing to do
      entry = new KSequenceIndexEntry();
      entry.index = kSequences.size();
      entry.lastTransactionId = -1;
      kSequence = new int[] { kSequence[0], kSequence[1] }; // copy necessary here 
      kSequences.add(kSequence);
      kSequenceIndex.put(kSequence, entry);
      //postingList = new TByteArrayList();
      postingList = new ByteArrayList();
      kPostingLists.add(postingList);
      kTotalSupports.add(0);
    } else {
      // a new occurrence of a previously seen 2-sequence
      postingList = kPostingLists.get(entry.index);
      if (kTotalSupports.get(entry.index) < minSupportOfPosting) return; // nothing to do
    }

    // add the current occurrence to the posting list
    if (entry.lastTransactionId != transactionId) {
      if (postingList.size() > 0) {
        // add a separator
        PostingList.addCompressed(0, postingList);
      }
      // add transaction id
      PostingList.addCompressed(transactionId + 1, postingList);
      PostingList.addCompressed(position + 1, postingList);

      // update data structures
      entry.lastTransactionId = transactionId;
      entry.lastPosition = position;
      int kTotalSupport = kTotalSupports.get(entry.index) + support;
      kTotalSupports.set(entry.index, kTotalSupport);
      if (bufferTransactions) {
        // we have seen a new transaction with item kSequence[0] as first item in a 2-sequence
        int maxTotalSupport = k2MaxLeftSupport.get(kSequence[0]);
        if (kTotalSupport > maxTotalSupport) {
          k2MaxLeftSupport.put(kSequence[0], kTotalSupport);
        }
      }
    } else if (entry.lastPosition != position) { // don't add any position more than once
      PostingList.addCompressed(position + 1, postingList);
      entry.lastPosition = position;
    }
  }

  /** Computes the number of items in between the items indicated by the 
   * start and end pointers. If there are more than gamma + 1 returns this distance instead
   * of computing it. Correctly treats gap entries (e.g., -3 indicating 3 irrelevant items).
   *
   * @param transaction input transaction
   * @param index1 index of first item in transaction
   * @param index2 index of second item in transaction
   * @param gamma maximum gap
   * @return the length of the gap or gamma + 1 if gap > gamma
   */
  public int computeGap(int[] transaction, int index1, int index2, int gamma) {
    if (index2 - index1 > gamma + 1) return gamma + 1; // quick check, for efficiency
    int gap = 0;
    for (int i = index1 + 1; i < index2; i++) {
      if (transaction[i] < 0) {
        gap -= transaction[i];
      } else {
        gap++;
      }
      if (gap > gamma) {
        return gap;
      }
    }
    return gap;
  }

  /** Finalizes the input phase by computing the overall support of each 2-sequence and pruning
   * all 2-sequences below minimum support. */
  private void finalizeInput(final FsmWriter writer) {
    if (bufferTransactions) {
      int remainingSupport = totalSupport;
      // now we build the 2-index
      for (int tId = 0; tId < inputTransactions.size(); tId++) {
        int[] transaction = inputTransactions.set(tId, null); // free space
        addTransactionToIndex(transaction, 0, transaction.length, 
            transactionSupports.get(tId), tId, remainingSupport);        
        remainingSupport -= transactionSupports.get(tId);
      }
      inputTransactions.clear();
      k1TotalSupports.clear();
      k2MaxLeftSupport.clear();
      itemsInTransactionTemp.clear();
    }
    
    // returning the 2-sequences that have support equal or above minsup and their posting lists
    kSequenceIndex.clear(); // not needed anymore
    //transactionSupports.trimToSize(); // will not be changed anymore
    transactionSupports.trim(); // will not be changed anymore

    // compute total support of each sequence and remove sequences with support less than sigma
    for (int id = 0; id < kSequences.size();) {
      if (kTotalSupports.get(id) >= sigma) {
        // accept sequence
        // uncomment next line to save some space during 1st phase (but: increased runtime)
        // postingList.trimToSize();
        id++; // next id 
      } else {
        // delete the current sequence (by moving the last sequence to the current position)
        int size1 = kPostingLists.size() - 1;
        if (id < size1) {
          kSequences.set(id, kSequences.remove(size1));
          kPostingLists.set(id, kPostingLists.remove(size1));
          kTotalSupports.set(id, kTotalSupports.get(size1));
          kTotalSupports.remove(size1);
          
        } else {
          kSequences.remove(size1);
          kPostingLists.remove(size1);
          kTotalSupports.remove(size1);
        }
        // same id again (now holding a different kSequence)
      }
    }
    
    if(this.outputType != Type.ALL && !fListMap.isEmpty()) { // Only for sequential mode
    	 switch(outputType){
         case MAXIMAL:
        	for(int i = 0; i < kSequences.size(); ++i) {
         		fListMap.removeKey(kSequences.get(i)[0]);
         		fListMap.removeKey(kSequences.get(i)[1]);
         	}
        	break;
         
         case CLOSED:
        	 for(int i = 0; i < kSequences.size(); ++i) {
         		if(fListMap.get(kSequences.get(i)[0]) == kTotalSupports.getInt(i)){
         			fListMap.removeKey(kSequences.get(i)[0]);
         		}
         		if(fListMap.get(kSequences.get(i)[1]) == kTotalSupports.getInt(i)){
         			fListMap.removeKey(kSequences.get(i)[1]);
         		}
         	}
           break;
         default:
           break;
         }
    	 
    	 //Output maximal (or closed 1-sequences)
    	 
		IntIntProcedure condition = new IntIntProcedure() { 
    		 public boolean apply(int key, int value) {
    			 if (writer != null)
					try {
						writer.write(new int[]{key}, value);
					} catch (Exception e) {
						e.printStackTrace();
					} 
    			 return true;
    		 }
    	 };
    	 
    	 fListMap.forEachPair(condition);
    }
        
  }

  // -- mining phase ------------------------------------------------------------------------------

  public void mineAndClear(FsmWriter writer) throws IOException, InterruptedException {
   
    finalizeInput(writer);

    while ((k < lambda) && !kSequences.isEmpty()) {
      // bfsMaxTraversal(writer);
      bfsTraversal(writer);
    }

    BitSet outputKSequence = new BitSet(kSequences.size());
    outputKSequences(writer, outputKSequence);
    clear();
  }

  /** Outputs all k-sequences that contain a pivot.
     *  
     * @throws InterruptedException 
     * @throws IOException */
  private void outputKSequences(FsmWriter writer, BitSet outputKSequence) throws IOException, InterruptedException {
    int[] prefixSequence = null;
    int[] temp = new int[k];

    // walk over all sequences
    for (int i = 0; i < kSequences.size(); i++) {
      int[] sequence = kSequences.get(i);

      // uncompress sequence (if necessary)
      if (k == 2 || sequence.length == k) {
        // uncompressed sequence 
        prefixSequence = sequence;
      } else {
        // compressed sequence (entries = (prefix pointer, suffix item)
        // reconstruct whole sequence by taking first k-1 symbols taken from previous sequence
        // plus the given suffix
        System.arraycopy(prefixSequence, 0, temp, 0, prefixSequence.length - 1);
        temp[k - 1] = sequence[1]; // suffix item
        sequence = temp;
      }

      // check if the sequence contains a pivot
      boolean hasPivot = false;
      for (int word : sequence) {
        if (word >= beginItem) {
          assert word <= endItem; // contract of this class
          hasPivot = true;
          break;
        }
      }
      
      // If the corresponding bit is set, we dont output the sequence
      if(outputKSequence.get(i))
        continue;

      // if contains a pivot, output the sequence and its support
      if (hasPivot) {
        if (writer != null) writer.write(sequence, kTotalSupports.get(i));
      }
    }
  }

  /**
   * This method constructs all frequent (k+1)-sequences from the set of k-sequences 
   * (values of k, kSequences, kPostings, kTotalSupport are updated).
   * @throws InterruptedException 
   * @throws IOException 
   */
  private void bfsTraversal(FsmWriter writer) throws IOException, InterruptedException {
    // terminology
    // k             : 5
    // k1            : 6
    // k- sequence   : abcde
    // prefix        : abcd       (= right join key)
    // suffix        :  bcde      (= left join key)
    
    
    // INTEGRATION -- begin
    // Does not outputs a sequence if the corresponding bit is set!
    BitSet outputKSequence = new BitSet(kSequences.size());

    // INTEGRATION -- end
    
    
    // build prefix/suffix indexes (maps prefix/suffix to list of sequences with this prefix/suffix)
    // values point to indexes in kSequences
    Map<IntArrayList, IntArrayList> sequencesWithSuffix = new Object2ObjectOpenHashMap<IntArrayList, IntArrayList>();
    Map<IntArrayList, IntArrayList> sequencesWithPrefix = new Object2ObjectOpenHashMap<IntArrayList, IntArrayList>();
    buildPrefixSuffixIndex(sequencesWithPrefix, sequencesWithSuffix);

    // variables for the (k+1)-sequences
    int k1 = k + 1;
    ArrayList<int[]> k1Sequences = new ArrayList<int[]>();
    ArrayList<ByteArrayList> k1PostingLists = new ArrayList<ByteArrayList>();
    IntArrayList k1TotalSupports = new IntArrayList();

    // temporary variables
    ByteArrayList postingList = new ByteArrayList(); // list of postings for a new (k+1) sequence    
    PostingList.Decompressor leftPostingList = new PostingList.Decompressor(); // posting list of the left k-sequence
    PostingList.Decompressor rightPostingList = new PostingList.Decompressor(); // posting list of the right k-sequence

    // we now join sequences (e.g., abcde) that end with some suffix with sequences
    // that start with the same prefix (e.g., bcdef)
    for (Map.Entry<IntArrayList, IntArrayList> entry : sequencesWithSuffix.entrySet()) {
      // if there is no right key to join, continue
      IntArrayList joinKey = entry.getKey();
      IntArrayList rightSequences = sequencesWithPrefix.get(joinKey); // indexes of right sequences
      if (rightSequences == null) {
        continue;
      }

      // there are right keys for the join, so let's join
      IntArrayList leftSequences = entry.getValue(); // indexes of left sequences
      for (int i = 0; i < leftSequences.size(); i++) {
        // get the postings of that sequence for joining
        leftPostingList.postingList = kPostingLists.get(leftSequences.get(i));        
        
        // compression
        // total number of successful joins for the current left sequence
        int noK1SequencesForLeftSequence = 0;
        int pointerToFirstK1Sequence = -1; // index of first join match

        // for every right key that matches the current left key, perform
        // a merge join of the posting lists (match if we find two postings
        // of the same transactions such that the starting position of the right
        // sequence is close enough to the starting position of the left 
        // sequence (at most gamma items in between)
        for (int j = 0; j < rightSequences.size(); j++) {
          // initialize
          postingList.clear();
          leftPostingList.offset = 0;
          rightPostingList.postingList = kPostingLists.get(rightSequences.get(j));
          rightPostingList.offset = 0;
          int totalSupport = 0; // of the result posting list
          int leftRemainingSupport = kTotalSupports.get(leftSequences.get(i)); // of left posting list (including current posting)
          int rightRemainingSupport = kTotalSupports.get(rightSequences.get(j)); // of right posting list (including current posting)          
          int leftTransactionId = leftPostingList.nextValue();
          int rightTransactionId = rightPostingList.nextValue();
          boolean foundMatchWithLeftTransactionId = false;

          while (leftPostingList.hasNextValue() && rightPostingList.hasNextValue() &&
              (totalSupport + Math.min(leftRemainingSupport, rightRemainingSupport) >= sigma) // early abort (can't become frequent)
              ) {
            // invariant: leftPostingList and rightPostingList point to first position after
            // a transaction id

            if (leftTransactionId == rightTransactionId) {
              // potential match; now check offsets
              int transactionId = leftTransactionId;
              int rightPosition = -1;
              while (leftPostingList.hasNextValue()) {
                int leftPosition = leftPostingList.nextValue();

                // fast forward right cursor (merge join; positions are sorted)
                while (rightPosition <= leftPosition && rightPostingList.hasNextValue()) {
                  rightPosition = rightPostingList.nextValue();
                }
                if (rightPosition <= leftPosition) break;

                // check whether join condition is met
                if (rightPosition <= leftPosition + gamma + 1) {
                  // yes, add a posting
                  if (!foundMatchWithLeftTransactionId) {
                    if (postingList.size() > 0) {
                      PostingList.addCompressed(0, postingList); // add separator byte
                    }
                    PostingList.addCompressed(transactionId + 1, postingList); // add transaction id
                    foundMatchWithLeftTransactionId = true;
                    totalSupport += transactionSupports.get(transactionId);
                  }
                  PostingList.addCompressed(leftPosition + 1, postingList); // add position 
                }
              }

              // advance both join lists
              if (rightPostingList.nextPosting()) {
                rightRemainingSupport -= transactionSupports.get(rightTransactionId);
                rightTransactionId = rightPostingList.nextValue();
              }
              if (leftPostingList.nextPosting()) {
                leftRemainingSupport -= transactionSupports.get(leftTransactionId);
                leftTransactionId = leftPostingList.nextValue();
                foundMatchWithLeftTransactionId = false;
              }
              // end leftTransactionId == rightTransactionId
            } else if (leftTransactionId > rightTransactionId) {
              // advance right join list (merge join; lists sorted by transaction id)
              if (rightPostingList.nextPosting()) {
                rightRemainingSupport -= transactionSupports.get(rightTransactionId);
                rightTransactionId = rightPostingList.nextValue();
              }
            } else {
              // advance left join (merge join; lists sorted by transaction id)
              if (leftPostingList.nextPosting()) {
                leftRemainingSupport -= transactionSupports.get(leftTransactionId);
                leftTransactionId = leftPostingList.nextValue();
                foundMatchWithLeftTransactionId = false;
              }
            }
          }

          // if the new (k+1)-sequence has support equal or above minimum support,
          // add it to the result of this round
          if (totalSupport >= sigma) {
            noK1SequencesForLeftSequence++;
            int suffixItem = this.kSequences.get(rightSequences.get(j))[this.kSequences.get(rightSequences.get(j)).length - 1];
            int[] kSequence; // holds result

            if (noK1SequencesForLeftSequence == 1) {
              // uncompressed output
              pointerToFirstK1Sequence = k1Sequences.size();

              // construct whole (k+1)-sequence
              kSequence = new int[k1];
              int[] prefix = kSequences.get(leftSequences.get(i));
              if (prefix.length == k1 - 1 || k1 <= 3) { // prefix sequence is uncompressed
                System.arraycopy(prefix, 0, kSequence, 0, prefix.length);
              } else { // prefix sequence is compressed (only suffix item stored)
                // need to retrieve prefix from initial sequence
                int prefixPos = prefix[0];
                int[] tempPrefix = kSequences.get(prefixPos);
                System.arraycopy(tempPrefix, 0, kSequence, 0, tempPrefix.length - 1);
                kSequence[k1 - 2] = prefix[1];
              }
              kSequence[k1 - 1] = suffixItem;
            } else {
              // include only the suffix item of (k+1)-sequence (first k items same as 
              // the ones at index pointerToPrefixSequence)
              kSequence = new int[2];
              kSequence[0] = pointerToFirstK1Sequence;
              kSequence[1] = suffixItem;
            }

            // store in results of current round
            k1Sequences.add(kSequence);
            ByteArrayList temp = new ByteArrayList(postingList.size()); // copying necessary here; postingList reused
            for (int k = 0; k < postingList.size(); k++) {
              temp.add(postingList.get(k)); // bad API here; newer Trove versions support this directly
            }
            k1PostingLists.add(temp);
            k1TotalSupports.add(totalSupport);
            
            //INTEGRATION -- begin
            
            switch(outputType){
            case MAXIMAL:
              outputKSequence.set(leftSequences.get(i));
              outputKSequence.set(rightSequences.get(j));
              break;
            
            case CLOSED:
              if (kTotalSupports.get(leftSequences.get(i)) == totalSupport) {
                outputKSequence.set(leftSequences.get(i));
              }
              if (kTotalSupports.get(rightSequences.get(j)) == totalSupport) {
                outputKSequence.set(rightSequences.get(j));
              }
              break;
            default:
              break;
            }
            
            //INTEGRATION -- end
            
          }
        } // for all right sequences  of the same key
      } // for all left sequences of each left key
    } // for all left keys

    
    // INTEGRATOIN -- begin
    //Delayed output for length k Sequences
    outputKSequences(writer, outputKSequence);
    
    // INTEGRATION -- end
    
    // we are done; store output
    k = k1;
    this.kSequences = k1Sequences;
    this.kPostingLists = k1PostingLists;
    this.kTotalSupports = k1TotalSupports;
  }

  /** Builds a prefix/suffix index from the currently stored k-sequences */
  void buildPrefixSuffixIndex(Map<IntArrayList, IntArrayList> sequencesWithPrefix, Map<IntArrayList, IntArrayList> sequencesWithSuffix) {
    int k1 = k + 1;

    // scan over all k-sequences and build prefix/suffix index
    IntArrayList suffix = null;
    IntArrayList prefix = null;
    for (int index = 0; index < kSequences.size(); index++) {
      int[] sequence = kSequences.get(index);

      // construct prefix (last item of sequence omitted) and suffix (first item omitted)
      if (sequence.length == 2 && k1 > 3) {
        // compressed sequence
        // only suffix in sequence, need to construct left key
        suffix = new IntArrayList(k - 1); // TODO: inefficient
        for (int j = 1; j < prefix.size(); j++) {
          suffix.add(prefix.get(j));
        }
        suffix.add(sequence[1]);
        // right key remains unchanged
      } else {
        // uncompressed sequence
        prefix = new IntArrayList(k - 1);
        for (int j = 0; j < k - 1; j++) {
          prefix.add(sequence[j]);
        }

        suffix = new IntArrayList(k - 1);
        for (int j = 1; j < k; j++) {
          suffix.add(sequence[j]);
        }
      }

      // update list of sequences starting with the prefix
      IntArrayList sequences = sequencesWithPrefix.get(prefix);
      if (sequences == null) {
        sequences = new IntArrayList();
        sequencesWithPrefix.put(prefix, sequences);
      }
      sequences.add(index);

      // update list of sequences ending with suffix
      sequences = sequencesWithSuffix.get(suffix);
      if (sequences == null) {
        sequences = new IntArrayList();
        sequencesWithSuffix.put(suffix, sequences);
      }
      sequences.add(index);
    }
  }

}
