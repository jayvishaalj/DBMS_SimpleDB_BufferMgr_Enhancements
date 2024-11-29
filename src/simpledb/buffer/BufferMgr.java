package simpledb.buffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BufferMgr {
   private Map<BlockId, Buffer> bufferPoolMap;
   private int numAvailable;
   private static final long MAX_TIME = 10000; // 10 seconds
   private int maxBuffers;
   private FileMgr fm;
   private LogMgr lm;
   private Map<BlockId, List<Integer>> accesses;
   Map<BlockId, Double> backwardDist;
   private Buffer head;
   private Buffer tail;
   private int time = 1;
   /**
    * Creates a buffer manager having the specified number
    * of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link SimpleDB.simpledb.log.LogMgr LogMgr} object.
    * @param numbuffs the number of buffer slots to allocate
    */
   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
      bufferPoolMap = new HashMap<>();
      numAvailable = numbuffs;
      maxBuffers = numbuffs;
      this.fm = fm;
      this.lm = lm;
      accesses = new HashMap<>();
      backwardDist = new HashMap<>();
      head = new Buffer(fm,lm);
      tail = new Buffer(fm,lm);
      head.next = tail;
      tail.prev = head;
   }

   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   public synchronized int available() {
      return numAvailable;
   }

   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   public synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferPoolMap.values()) {
         if (buff.modifyingTx() == txnum) {
            buff.flush();
         }
      }
   }


   /**
    * Unpins the specified data buffer. If its pin count
    * goes to zero, then notify any waiting threads.
    * @param buff the buffer to be unpinned
    */
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable++;
         notifyAll();
      }
   }


   /**
    * Pins a buffer to the specified block, potentially
    * waiting until a buffer becomes available.
    * If no buffer becomes available within a fixed
    * time period, then a {@link BufferAbortException} is thrown.
    * @param blk a reference to a disk block
    * @return the buffer pinned to that block
    */
   public synchronized Buffer pin(BlockId blk) {
      try {
         long timestamp = System.currentTimeMillis();
         Buffer buff = tryToPin(blk);

         while (buff == null && !waitingTooLong(timestamp)) {
            wait(MAX_TIME);
            buff = tryToPin(blk);
         }

         if (buff == null) {
            throw new BufferAbortException();
         }
         return buff;

      } catch (InterruptedException e) {
         throw new BufferAbortException();
      }
   }

   private boolean waitingTooLong(long starttime) {
      return System.currentTimeMillis() - starttime > MAX_TIME;
   }

   /**
    * Tries to pin a buffer to the specified block.
    * If there is already a buffer assigned to that block
    * then that buffer is used;
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   private Buffer tryToPin(BlockId blk) {
      Buffer buff = bufferPoolMap.get(blk);

      // If block is not already in the pool, allocate or replace a buffer
      if (buff == null) {
         System.out.println("Blk not in buffer pool map");
         if (bufferPoolMap.size() < maxBuffers) {
            System.out.println("Buffer pool map size less than max buffers");
            buff = new Buffer(this.fm, this.lm); // Create a new buffer
            System.out.println("Created buffer: "+buff);
         } else {
            System.out.println("Choosing unpinned buffer");
            buff = chooseUnpinnedBuffer(); // Replace an existing unpinned buffer
            if (buff == null) {
               System.out.println("No buffer available");
               return null; // No buffer available
            }
            System.out.println("Chose to replace "+buff);
            bufferPoolMap.remove(buff.block()); // Remove old block from the map
            dequeue(buff);
            backwardDist.remove(buff.block());
         }

         // Assign the block to the buffer
         buff.assignToBlock(blk);
         bufferPoolMap.put(blk, buff);
         enqueue(buff);
      } else {
         dequeue(buff);
         enqueue(buff);
         System.out.println("Queue: ");
         displayQueue();
         System.out.println();
      }

      // Pin the buffer
      if (!buff.isPinned()) {
         numAvailable--;
      }
      buff.pin();

      updateAccesses(blk);

      return buff;
   }

   public Buffer findExistingBuffer(BlockId blk) {

      return this.bufferPoolMap.get(blk);
   }

   private Buffer chooseUnpinnedBuffer() {
      List<Double> distances = new ArrayList<>(backwardDist.values());
      Collections.sort(distances, Collections.reverseOrder());

      if (distances.get(0) == Double.POSITIVE_INFINITY) {
         Buffer evict = findFirstUnpinnedInfinityBuffer();
         if (evict != null) {
            return evict;
         }
      }

      for (Double dist : distances) {
         for (BlockId b : backwardDist.keySet()) {
            if (backwardDist.get(b) == dist && !bufferPoolMap.get(b).isPinned()) {
               Buffer evict = bufferPoolMap.get(b);
               return evict;
            }
         }
      }
      return null;
   }

   private void enqueue(Buffer b) {
      b.next = tail;
      b.prev = tail.prev;

      tail.prev.next = b;
      tail.prev = b;
   }

   private void dequeue(Buffer b) {
      b.prev.next = b.next;
      b.next.prev = b.prev;
   }

   private void updateAccesses(BlockId blk) {
      List<Integer> access = accesses.get(blk);

      if(access == null) {//block never accessed before
         List<Integer> acc = new ArrayList<>();
         acc.add(time++);
         acc.add(-1);
         acc.add(-1);
         accesses.put(blk,acc);

         updateBackwardDistance(blk,Double.POSITIVE_INFINITY);
      } else {
         if (access.contains(-1)) {//block accessed less than 3 times
            if (access.get(1) == -1) {
               access.remove(1);
               access.add(1,time++);

               updateBackwardDistance(blk,Double.POSITIVE_INFINITY);
            } else {
               access.remove(2);
               access.add(time++);

               Integer backwardDistance = access.get(2) - access.get(0);
               updateBackwardDistance(blk,backwardDistance.doubleValue());
            }
         } else {//block accessed at least 3 times
            access.remove(0);
            access.add(time++);

            Integer backwardDistance = access.get(2) - access.get(0);
            updateBackwardDistance(blk,backwardDistance.doubleValue());
         }
         accesses.replace(blk,access);
      }
   }

   private void updateBackwardDistance(BlockId blk, Double dist) {
      if (backwardDist.containsKey(blk)) {
         backwardDist.replace(blk,dist);
      } else {
         backwardDist.put(blk,dist);
      }
      for (BlockId b : backwardDist.keySet()) {
         Double d = backwardDist.get(b) + 1;
         if (b != blk) {
            backwardDist.replace(b,d);
         }
      }
   }

   private Buffer findFirstUnpinnedInfinityBuffer() {
      Buffer buff = head.next;
      while (buff != tail) {
         if (backwardDist.get(buff.block()) == Double.POSITIVE_INFINITY && !buff.isPinned()) {
            return buff;
         }
         buff = buff.next;
      }
      return null;
   }

   private void displayQueue() {
      Buffer buff = head.next;
      while(buff != tail) {
         System.out.println(buff);
         buff = buff.next;
      }
   }
}
