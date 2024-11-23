//package simpledb.buffer;
//
//import simpledb.file.*;
//import simpledb.log.LogMgr;
//
///**
// * Manages the pinning and unpinning of buffers to blocks.
// * @author Edward Sciore
// *
// */
//public class BufferMgr {
//   private Buffer[] bufferpool;
//   private int numAvailable;
//   private static final long MAX_TIME = 10000; // 10 seconds
//   
//   /**
//    * Creates a buffer manager having the specified number 
//    * of buffer slots.
//    * This constructor depends on a {@link FileMgr} and
//    * {@link simpledb.log.LogMgr LogMgr} object.
//    * @param numbuffs the number of buffer slots to allocate
//    */
//   public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
//      bufferpool = new Buffer[numbuffs];
//      numAvailable = numbuffs;
//      for (int i=0; i<numbuffs; i++)
//         bufferpool[i] = new Buffer(fm, lm);
//   }
//   
//   /**
//    * Returns the number of available (i.e. unpinned) buffers.
//    * @return the number of available buffers
//    */
//   public synchronized int available() {
//      return numAvailable;
//   }
//   
//   /**
//    * Flushes the dirty buffers modified by the specified transaction.
//    * @param txnum the transaction's id number
//    */
//   public synchronized void flushAll(int txnum) {
//      for (Buffer buff : bufferpool)
//         if (buff.modifyingTx() == txnum)
//         buff.flush();
//   }
//   
//   
//   /**
//    * Unpins the specified data buffer. If its pin count
//    * goes to zero, then notify any waiting threads.
//    * @param buff the buffer to be unpinned
//    */
//   public synchronized void unpin(Buffer buff) {
//      buff.unpin();
//      if (!buff.isPinned()) {
//         numAvailable++;
//         notifyAll();
//      }
//   }
//   
//   /**
//    * Pins a buffer to the specified block, potentially
//    * waiting until a buffer becomes available.
//    * If no buffer becomes available within a fixed 
//    * time period, then a {@link BufferAbortException} is thrown.
//    * @param blk a reference to a disk block
//    * @return the buffer pinned to that block
//    */
//   public synchronized Buffer pin(BlockId blk) {
//      try {
//         long timestamp = System.currentTimeMillis();
//         Buffer buff = tryToPin(blk);
//         while (buff == null && !waitingTooLong(timestamp)) {
//            wait(MAX_TIME);
//            buff = tryToPin(blk);
//         }
//         if (buff == null)
//            throw new BufferAbortException();
//         return buff;
//      }
//      catch(InterruptedException e) {
//         throw new BufferAbortException();
//      }
//   }  
//   
//   private boolean waitingTooLong(long starttime) {
//      return System.currentTimeMillis() - starttime > MAX_TIME;
//   }
//   
//   /**
//    * Tries to pin a buffer to the specified block. 
//    * If there is already a buffer assigned to that block
//    * then that buffer is used;  
//    * otherwise, an unpinned buffer from the pool is chosen.
//    * Returns a null value if there are no available buffers.
//    * @param blk a reference to a disk block
//    * @return the pinned buffer
//    */
//   public Buffer tryToPin(BlockId blk) {
//      Buffer buff = findExistingBuffer(blk);
//      if (buff == null) {
//         buff = chooseUnpinnedBuffer();
//         if (buff == null)
//            return null;
//         buff.assignToBlock(blk);
//      }
//      if (!buff.isPinned())
//         numAvailable--;
//      buff.pin();
//      return buff;
//   }
//   
//   public Buffer findExistingBuffer(BlockId blk) {
//      for (Buffer buff : bufferpool) {
//         BlockId b = buff.block();
//         if (b != null && b.equals(blk))
//            return buff;
//      }
//      return null;
//   }
//   
//   public Buffer chooseUnpinnedBuffer() {
//      for (Buffer buff : bufferpool)
//         if (!buff.isPinned())
//         return buff;
//      return null;
//   }
//}

package simpledb.buffer;

import java.util.HashMap;
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
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} object.
    * @param numbuffs the number of buffer slots to allocate
    */
    public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
        bufferPoolMap = new HashMap<>();
        numAvailable = numbuffs;
        maxBuffers = numbuffs;
        this.fm = fm;
        this.lm = lm;
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
           if (bufferPoolMap.size() < maxBuffers) {
               buff = new Buffer(this.fm, this.lm); // Create a new buffer
           } else {
               buff = chooseUnpinnedBuffer(); // Replace an existing unpinned buffer
               if (buff == null) {
                   return null; // No buffer available
               }
               bufferPoolMap.remove(buff.block()); // Remove old block from the map
           }

           // Assign the block to the buffer
           buff.assignToBlock(blk);
           bufferPoolMap.put(blk, buff);
       }

       // Pin the buffer
       if (!buff.isPinned()) {
           numAvailable--;
       }
       buff.pin();
       return buff;
   }
   
   public Buffer findExistingBuffer(BlockId blk) {
	   
      return this.bufferPoolMap.get(blk);
   }
   
   private Buffer chooseUnpinnedBuffer() {
       for (Buffer buff : bufferPoolMap.values()) {
           if (!buff.isPinned()) {
               return buff;
           }
       }
       return null;
   }
}
