package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * An individual buffer. A databuffer wraps a page 
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 * @author Edward Sciore
 */
public class Buffer {
   private FileMgr fm;
   private LogMgr lm;
   private Page contents;
   private BlockId blk = null;
   private int pins = 0;
   private int txnum = -1;
   private int lsn = -1;
   public Buffer next;
   public Buffer prev;
   public Boolean dirty;

   public Buffer(FileMgr fm, LogMgr lm) {
      this.fm = fm;
      this.lm = lm;
      contents = new Page(fm.blockSize());
      this.next = null;
      this.prev = null;
      this.dirty = false;
      
   }
   
   public Page contents() {
      return contents;
   }

   /**
    * Returns a reference to the disk block
    * allocated to the buffer.
    * @return a reference to a disk block
    */
   public BlockId block() {
      return blk;
   }

   public synchronized void setModified(int txnum, int lsn) {
	   this.dirty = true;
      this.txnum = txnum;
      if (lsn >= 0)
         this.lsn = lsn;
   }

   /**
    * Return true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   public synchronized boolean isPinned() {
      return pins > 0;
   }
   
   // Method to check if the buffer is dirty
   public synchronized boolean isDirty() {
       return dirty;
   }
   
   public synchronized int modifyingTx() {
      return txnum;
   }

   /**
    * Reads the contents of the specified block into
    * the contents of the buffer.
    * If the buffer was dirty, then its previous contents
    * are first written to disk.
    * @param b a reference to the data block
    */
   synchronized void assignToBlock(BlockId b) {
      flush();
      blk = b;
      fm.read(blk, contents);
      pins = 0;
   }
   
   /**
    * Write the buffer to its disk block if it is dirty.
    */
   synchronized void flush() {
      if (txnum >= 0) {
         lm.flush(lsn);
         fm.write(blk, contents);
         txnum = -1;
         dirty = false;
      }
   }

   /**
    * Increase the buffer's pin count.
    */
   synchronized void pin() {
      pins++;
   }

   /**
    * Decrease the buffer's pin count.
    */
   synchronized void unpin() {
      pins--;
   }
}