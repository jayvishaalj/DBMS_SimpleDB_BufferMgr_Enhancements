package simpledb.buffer;

import org.junit.Before;
import org.junit.Test;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import java.io.File;

import static org.junit.Assert.*;

public class BufferMgrTest {
    private BufferMgr bufferMgr;
    private FileMgr fileMgr;
    private LogMgr logMgr;
    private static final int NUM_BUFFERS = 3;

    private BlockId block1, block2, block3, block4;

    @Before
    public void setUp() {
        // Initialize the FileMgr and LogMgr with a temporary directory
        File tempDir = new File("tempdb");
        tempDir.mkdir(); // Create the directory for testing
        fileMgr = new FileMgr(tempDir, 400); // Block size = 400 bytes
        logMgr = new LogMgr(fileMgr, "testlog");

        // Create a BufferMgr with 3 buffers
        bufferMgr = new BufferMgr(fileMgr, logMgr, NUM_BUFFERS);

        // Create test blocks
        block1 = new BlockId("testfile", 1);
        block2 = new BlockId("testfile", 2);
        block3 = new BlockId("testfile", 3);
        block4 = new BlockId("testfile", 4);
    }

    @Test
    public void testPinUnpin() {
        Buffer buffer = bufferMgr.pin(block1);
        assertNotNull(buffer); // Buffer should not be null
        assertTrue(buffer.isPinned()); // Buffer should be pinned

        bufferMgr.unpin(buffer);
        assertFalse(buffer.isPinned()); // Buffer should be unpinned
    }
   
    @Test
    public void testBufferAllocation() {
        for (int i = 0; i < NUM_BUFFERS; i++) {
            BlockId block = new BlockId("testfile", i);
            Buffer buffer = bufferMgr.pin(block);
            assertNotNull("Buffer should be allocated for each block", buffer);
        }

        try {
            BlockId block = new BlockId("testfile", NUM_BUFFERS);
            bufferMgr.pin(block);
            fail("BufferAbortException should be thrown when no buffers are available");
        } catch (BufferAbortException e) {
            // Expected exception
        }
    }
    
    @Test
    public void testBufferAvailability() {
        // Ensure all buffers are available initially
        int NUM_BUFFERS = 3; // Number of buffers in the buffer pool
        assertEquals("All buffers should be available initially", NUM_BUFFERS, bufferMgr.available());

        // Pin block1, reducing the number of available buffers
        Buffer buffer1 = bufferMgr.pin(block1);
        assertEquals("One buffer should be unavailable after pinning", NUM_BUFFERS - 1, bufferMgr.available());

        // Pin block2, further reducing availability
        Buffer buffer2 = bufferMgr.pin(block2);
        assertEquals("Two buffers should be unavailable after pinning another block", NUM_BUFFERS - 2, bufferMgr.available());

        // Pin block3, leaving no available buffers
        Buffer buffer3 = bufferMgr.pin(block3);
        assertEquals("No buffers should be available after pinning all blocks", 0, bufferMgr.available());

        bufferMgr.unpin(buffer1);
        assertEquals("One buffer should be available again after unpinning", 1, bufferMgr.available());

        // Unpin block2 and block3, making all buffers available

        bufferMgr.unpin(buffer2);

        bufferMgr.unpin(buffer3);
        assertEquals("All buffers should be available after unpinning all blocks", NUM_BUFFERS, bufferMgr.available());
    }


    @Test
    public void testDirtyBufferFlush() {
        // Pin block1 and mark it as dirty
        Buffer buffer = bufferMgr.pin(block1);
        buffer.setModified(100, 10); // Mark buffer as dirty
        bufferMgr.unpin(buffer);

        // Pin block2, which should cause a replacement if needed
        Buffer buffer2 = bufferMgr.pin(block2);
        // Ensure block1 was replaced by checking that block2 is now pinned
        assertTrue(buffer2.isPinned());
        assertEquals(block2, buffer2.block());

        // Confirm block1 was flushed by ensuring the buffer for block1 is now unpinned
        Buffer bufferForBlock1 = bufferMgr.pin(block1); // Re-pin block1
        assertNotNull(bufferForBlock1); // Block1 should still be accessible
        bufferMgr.unpin(bufferForBlock1); // Clean up
    }

    @Test
    public void testBufferLookup() {
        // Pin block1 and block2
        Buffer buffer1 = bufferMgr.pin(block1);
        Buffer buffer2 = bufferMgr.pin(block2);

        // Lookup block1 (should be in the buffer pool map)
        Buffer lookupBuffer = bufferMgr.findExistingBuffer(block1);
        assertSame(buffer1, lookupBuffer); // Should return the same buffer object

        // Unpin both buffers
        bufferMgr.unpin(buffer1);
        bufferMgr.unpin(buffer2);
    }

    @Test
    public void testLRUKReplacementPolicy() {
        // Pin 3 blocks (fills the buffer pool)
        Buffer buffer1 = bufferMgr.pin(block1);
        bufferMgr.unpin(buffer1);

        Buffer buffer2 = bufferMgr.pin(block2);
        bufferMgr.unpin(buffer2);

        Buffer buffer3 = bufferMgr.pin(block3);
        bufferMgr.unpin(buffer3);

        // Access block1 and block2 again to update their access history
        bufferMgr.pin(block1);
        bufferMgr.unpin(buffer1);

        bufferMgr.pin(block2);
        bufferMgr.unpin(buffer2);

        // Pin block4 (causing replacement of buffer with block3)
        Buffer buffer4 = bufferMgr.pin(block4);
        assertEquals(block4, buffer4.block());
        // Try to pin block3 again
        Buffer bufferForBlock3 = bufferMgr.pin(block3);

        // Ensure block3 is now assigned to a new or reassigned buffer
        assertNotSame(buffer4, bufferForBlock3); // Ensure block4's buffer is not reused for block3
        assertEquals(block3, bufferForBlock3.block()); // Ensure block3 is now in the buffer
    }

    @Test(expected = BufferAbortException.class)
    public void testBufferAbortOnExcessivePinRequests() {
        // Pin 3 blocks to fill the buffer pool
        bufferMgr.pin(block1);
        bufferMgr.pin(block2);
        bufferMgr.pin(block3);

        // Attempt to pin a fourth block without unpinning any (should timeout)
        bufferMgr.pin(block4);
    }
    
    @Test
    public void testPinnedBuffersNotEvicted() {
        // Pin 3 blocks
        Buffer buffer1 = bufferMgr.pin(block1);
        Buffer buffer2 = bufferMgr.pin(block2);
        Buffer buffer3 = bufferMgr.pin(block3);

        // Unpin only block2 and block3
        bufferMgr.unpin(buffer2);
        bufferMgr.unpin(buffer3);

        // Pin block4 (requires eviction)
        Buffer buffer4 = bufferMgr.pin(block4);

        // Ensure block1 is not evicted as it is still pinned
        assertNotNull(bufferMgr.findExistingBuffer(block1));
        assertEquals(block4, buffer4.block());
    }

    
    @Test
    public void testBackwardDistanceUpdate() {
        // Pin and unpin blocks to update backward distances
        bufferMgr.pin(block1);
        bufferMgr.unpin(bufferMgr.findExistingBuffer(block1));

        bufferMgr.pin(block2);
        bufferMgr.unpin(bufferMgr.findExistingBuffer(block2));

        bufferMgr.pin(block1); // Second access for block1
        bufferMgr.unpin(bufferMgr.findExistingBuffer(block1));

        // Check that backward distances are updated correctly
        assertTrue(bufferMgr.backwardDist.get(block1) > 0);
        assertEquals(Double.POSITIVE_INFINITY, bufferMgr.backwardDist.get(block2), 0.0);
    }
    
    @Test
    public void testFlushDirtyBuffersBeforeEviction() {
        // Pin block1 and mark it as dirty
        Buffer buffer1 = bufferMgr.pin(block1);
        buffer1.setModified(1, 1); // Mark as dirty
        bufferMgr.unpin(buffer1);

        // Pin block2
        Buffer buffer2 = bufferMgr.pin(block2);
        bufferMgr.unpin(buffer2);

        // Pin block3
        Buffer buffer3 = bufferMgr.pin(block3);
        bufferMgr.unpin(buffer3);

        // Pin block4 (requires eviction of a dirty buffer)
        Buffer buffer4 = bufferMgr.pin(block4);
        bufferMgr.unpin(buffer4);

        // Ensure block1 was flushed and evicted
        assertNull(bufferMgr.findExistingBuffer(block1)); // Block1 should not be in the buffer pool
        assertEquals(block4, buffer4.block());           // Block4 should be in the buffer pool
    }


    @Test
    public void testAccessPatternStress() {
        for (int i = 1; i <= 1000; i++) {
            BlockId block = new BlockId("stressfile", i % 10); // Reuse blocks
            bufferMgr.pin(block);
            bufferMgr.unpin(bufferMgr.findExistingBuffer(block));
        }

        assertTrue(bufferMgr.available() > 0); // Ensure some buffers are available
    }


    @Test
    public void testLRUKReplacementPolicyUpdated() {
        // Pin 3 blocks (fills the buffer pool)
        Buffer buffer1 = bufferMgr.pin(block1);
        bufferMgr.unpin(buffer1);

        Buffer buffer2 = bufferMgr.pin(block2);
        bufferMgr.unpin(buffer2);

        Buffer buffer3 = bufferMgr.pin(block3);
        bufferMgr.unpin(buffer3);

        // Access block1 and block2 again to update their access history
        bufferMgr.pin(block1);
        bufferMgr.unpin(buffer1);

        bufferMgr.pin(block2);
        bufferMgr.unpin(buffer2);

        // Pin block4 (causing replacement of buffer with block3)
        Buffer buffer4 = bufferMgr.pin(block4);
        assertEquals(block4, buffer4.block());
        assertNull(bufferMgr.findExistingBuffer(block3)); // Ensure block3 has been replaced // mine

        // Try to pin block3 again
        Buffer bufferForBlock3 = bufferMgr.pin(block3);
        bufferMgr.unpin(bufferForBlock3); // mine

        // Ensure block3 is now assigned to a new or reassigned buffer
        assertNotSame(buffer4, bufferForBlock3); // Ensure block4's buffer is not reused for block3
        assertNull(bufferMgr.findExistingBuffer(block1)); // Ensure block1 is not in the buffer // mine
        assertEquals(block3, bufferForBlock3.block()); // Ensure block3 is now in the buffer

        // Access block2 again to update access history // mine
        Buffer bufferForBlock2 = bufferMgr.pin(block2);
        bufferMgr.unpin(bufferForBlock2);

        assertEquals(block2, bufferForBlock2.block()); // Ensure block2 is still in the buffer // mine

        // Access block3 again to update access history // mine
        Buffer currentBufferForBlock3 = bufferMgr.pin(block3);
        bufferMgr.unpin(currentBufferForBlock3);

        assertEquals(block3, currentBufferForBlock3.block()); // Ensure block3 is still in the buffer // mine

        // Pin block1 (causing replacement of buffer with block4) // mine
        Buffer bufferForBlock1 = bufferMgr.pin(block1);
        bufferMgr.unpin(bufferForBlock1);

        assertNull(bufferMgr.findExistingBuffer(block2)); // Ensure block2 is not in the buffer // mine
        assertEquals(block1, bufferForBlock1.block()); // Ensure block1 is in the buffer // mine
    }
}


//package simpledb.buffer;
//
//import simpledb.server.SimpleDB;
//import simpledb.file.*;
//
//public class BufferMgrTest {
//   public static void main(String[] args) throws Exception {
//      SimpleDB db = new SimpleDB("buffermgrtest", 400, 3); // only 3 buffers
//      BufferMgr bm = db.bufferMgr();
//
//      Buffer[] buff = new Buffer[6]; 
//      buff[0] = bm.pin(new BlockId("testfile", 0));
//      buff[1] = bm.pin(new BlockId("testfile", 1));
//      buff[2] = bm.pin(new BlockId("testfile", 2));
//      bm.unpin(buff[1]); buff[1] = null;
//      buff[3] = bm.pin(new BlockId("testfile", 0)); // block 0 pinned twice
//      buff[4] = bm.pin(new BlockId("testfile", 1)); // block 1 repinned
//      System.out.println("Available buffers: " + bm.available());
//      try {
//         System.out.println("Attempting to pin block 3...");
//         buff[5] = bm.pin(new BlockId("testfile", 3)); // will not work; no buffers left
//      }
//      catch(BufferAbortException e) {
//         System.out.println("Exception: No available buffers\n");
//      }
//      bm.unpin(buff[2]); buff[2] = null;
//      buff[5] = bm.pin(new BlockId("testfile", 3)); // now this works
//
//      System.out.println("Final Buffer Allocation:");
//      for (int i=0; i<buff.length; i++) {
//         Buffer b = buff[i];
//         if (b != null) 
//            System.out.println("buff["+i+"] pinned to block " + b.block());
//      }
//   }
//}
