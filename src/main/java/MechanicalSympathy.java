
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class MechanicalSympathy
{
   public final static int numIterations = 1024 * 1024 * 8;
   public final static int queueSize = 1024 * 8;
   public final static int numRuns = 10;
   
   public final static Long[] values = new Long[numIterations];
   public final static long valuesSum;
   static
   {
      long sum = 0;
      Random r = new Random();
      for (int i = 0; i < numIterations; i++)
      {
         final long cur = r.nextLong();
         sum += cur;
         values[i] = cur;
      }
      valuesSum = sum;
   }
   
   public static interface SimpleQueue<E>
   {
      public boolean offer(E o);
      public E poll();
   }
   
   private void assertEquals(Object o1, Object o2)
   {
      if (!o1.equals(o2))
         System.out.println("ERROR: " + o2 + " didn't match the expected value " + o1 );
   }

   //=====================================================================================
   
   private long run(final Queue<Long> q, String prefix) throws Throwable
   {
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicLong valueSum = new AtomicLong();

      Thread thread = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            long sum = 0;
            Long cur;
            for (int i = 0; i < numIterations; i++)
            {
               while ((cur = q.poll()) == null) Thread.yield();
               sum += cur;
            }
            
            valueSum.set(sum);
            latch.countDown();
         }
      });
      
      thread.start();
      Thread.sleep(100);
      
      long startTime = System.currentTimeMillis();
      
      for (int i = 0;i < numIterations; i++)
         while (!q.offer(values[i])) Thread.yield();
      
      latch.await();
      long endTime = System.currentTimeMillis();
      
      assertEquals(valuesSum,valueSum.get());
      final long ret = (long)((double)((long)numIterations * 1000L)/(double)(endTime - startTime));
      System.out.format("%s %,d ops/sec%n", prefix, ret);
      return ret;
   }
   
   private long run(final SimpleQueue<Long> q, String prefix) throws Throwable
   {
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicLong valueSum = new AtomicLong();

      Thread thread = new Thread(new Runnable()
      {
         @Override
         public void run()
         {
            long sum = 0;
            Long cur;
            for (int i = 0; i < numIterations; i++)
            {
               while ((cur = q.poll()) == null) Thread.yield();
               sum += cur;
            }
            
            valueSum.set(sum);
            latch.countDown();
         }
      });
      
      thread.start();
      Thread.sleep(100);
      
      long startTime = System.currentTimeMillis();
      
      for (int i = 0;i < numIterations; i++)
         while (!q.offer(values[i])) Thread.yield();
      
      latch.await();
      long endTime = System.currentTimeMillis();
      
      assertEquals(valuesSum,valueSum.get());
      final long ret = (long)((double)((long)numIterations * 1000L)/(double)(endTime - startTime));
      System.out.format("%s %,d ops/sec%n", prefix, ret);
      return ret;
   }
   
   public void runTests(final Queue<Long> q, String prefix) throws Throwable
   {
      long average = 0;
      for (int i = 0; i < numRuns; i++)
         average += run(q, prefix);
      System.out.format("%s Average: %,d ops/sec%n", prefix, (average / (long)numRuns));
   }
   
   public void runTests(final SimpleQueue<Long> q, String prefix) throws Throwable
   {
      long average = 0;
      for (int i = 0; i < numRuns; i++)
         average += run(q, prefix);
      System.out.format("%s Average: %,d ops/sec%n", prefix, (average / (long)numRuns));
   }
   
   //=====================================================================================
   
   public void testBlockingQueue() throws Throwable
   {
      runTests(new ArrayBlockingQueue<Long>(queueSize), "BlockingQueue");
   }
   
   //=====================================================================================
   
   public static final class OneToOneQueueBlocking<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      private long tail = 0;
      private long head = 0;
      
      @SuppressWarnings("unchecked")
      public OneToOneQueueBlocking(int size)
      {
         buffer = (E[])(new Object[size]);
      }
      
      public final synchronized boolean offer(E o)
      {
         final long wrapPoint = tail - buffer.length;
         if (head <= wrapPoint)
            return false;
         buffer[(int)(tail % buffer.length)] = o;
         tail++;
         return true;
      }
      
      public final synchronized E poll()
      {
         if (head >= tail)
            return null;
         final int index = (int)(head % buffer.length);
         head++;
         return buffer[index];
      }
   }
   
   public void testSynchronized() throws Throwable
   {
      runTests(new OneToOneQueueBlocking<Long>(queueSize), "Synchronized");
   }
   
   //=====================================================================================
   
   public void testConcurrentQueue() throws Throwable
   {
      runTests(new ConcurrentLinkedQueue<Long>(), "ConcurrentQueue");
   }
   
   //=====================================================================================
   
   public static class OneToOneQueueVolatile<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      private volatile long tail = 0;
      private volatile long head = 0;
      
      @SuppressWarnings("unchecked")
      public OneToOneQueueVolatile(int size)
      {
         buffer = (E[])(new Object[size]);
      }
      
      public final boolean offer(final E o)
      {
         final long curTail = tail; // read volatile.
         final long wrapPoint = curTail - buffer.length;
         if (head <= wrapPoint) // read volatile
            return false;
         
         buffer[(int)(curTail % buffer.length)] = o;
         
         tail = curTail + 1; // write volatile - happens before semantics
         
         return true;
      }
      
      public final E poll()
      {
         final long curHead = head; // volatile read
         
         if (curHead >= tail) // volatile read
            return null;
         
         final int index = (int)(curHead % buffer.length);
         final E ret = buffer[index];
         
         head = curHead + 1; // write volatile
         
         return ret;
      }
   }

   public void testVolatile() throws Throwable
   {
      runTests(new OneToOneQueueVolatile<Long>(queueSize),"Volatile");
   }
   
   //=====================================================================================
   
   public static final class OneToOneQueueSoftMB<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      private final AtomicLong tail = new AtomicLong(0);
      private final AtomicLong head = new AtomicLong(0);
      
      @SuppressWarnings("unchecked")
      public OneToOneQueueSoftMB(int size)
      {
         buffer = (E[])(new Object[size]);
      }
      
      public final boolean offer(final E o)
      {
         final long curTail = tail.get();
         final long wrapPoint = curTail - buffer.length;
         if (head.get() <= wrapPoint)
            return false;
         
         buffer[(int)(curTail % buffer.length)] = o;
         
         tail.lazySet(curTail + 1); // StoreStore memory barrier
         
         return true;
      }
      
      public final E poll()
      {
         final long curHead = head.get();
         
         if (curHead >= tail.get())
            return null;
         
         final int index = (int)(curHead % buffer.length);
         final E ret = buffer[index];
         
         head.lazySet(curHead + 1); // StoreStore memory barrier
         
         return ret;
      }
   }

   public void testSoftMB() throws Throwable
   {
      runTests(new OneToOneQueueSoftMB<Long>(queueSize),"SoftMB");
   }
   
   //=====================================================================================
   
   public static final class OneToOneQueueSoftMBMod<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      private final AtomicLong tail = new AtomicLong(0);
      private final AtomicLong head = new AtomicLong(0);
      
      private final int bufferSize;
      private final int indexMask;

      
      @SuppressWarnings("unchecked")
      public OneToOneQueueSoftMBMod(int size)
      {
         if (Integer.bitCount(size) != 1)
            throw new IllegalArgumentException("bufferSize must be a power of 2");
         
         this.bufferSize = size;
         this.indexMask = size - 1;

         buffer = (E[])(new Object[size]);
      }
      
      public final boolean offer(final E o)
      {
         final long curTail = tail.get();
         final long wrapPoint = curTail - bufferSize;
         if (head.get() <= wrapPoint)
            return false;
         
         buffer[(int)(curTail & indexMask)] = o;
         
         tail.lazySet(curTail + 1); // StoreStore memory barrier
         
         return true;
      }
      
      public final E poll()
      {
         final long curHead = head.get();
         
         if (curHead >= tail.get())
            return null;
         
         final int index = (int)(curHead & indexMask);
         final E ret = buffer[index];
         
         head.lazySet(curHead + 1); // StoreStore memory barrier
         
         return ret;
      }
   }

   public void testSoftMBMod() throws Throwable
   {
      runTests(new OneToOneQueueSoftMBMod<Long>(queueSize),"SoftMBMod");
   }

   //=====================================================================================
   
   public static final class OneToOneQueueCachedHeadTail<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      private final AtomicLong tail = new PaddedAtomicLong(0);
      private final AtomicLong head = new PaddedAtomicLong(0);
      
      private long tailCache = 0; // used in poll
      private long headCache = 0; // used in offer

      private final int bufferSize;
      private final int indexMask;
      
      @SuppressWarnings("unchecked")
      public OneToOneQueueCachedHeadTail(int size)
      {
         if (Integer.bitCount(size) != 1)
            throw new IllegalArgumentException("bufferSize must be a power of 2");
         
         this.bufferSize = size;
         this.indexMask = size - 1;

         buffer = (E[])(new Object[size]);
      }
      
      public final boolean offer(final E o)
      {
         final long curTail = tail.get();
         final long wrapPoint = curTail - bufferSize;
         if (headCache <= wrapPoint)
         {
            headCache = head.get();
            if (headCache <= wrapPoint)
               return false;
         }
         
         buffer[(int)(curTail & indexMask)] = o;
         
         tail.lazySet(curTail + 1); // StoreStore memory barrier
         
         return true;
      }
      
      public final E poll()
      {
         final long curHead = head.get();
         
         if (curHead >= tailCache)
         {
            tailCache = tail.get();
            if (curHead >= tailCache)
               return null;
         }
         
         final int index = (int)(curHead & indexMask);
         final E ret = buffer[index];
         
         head.lazySet(curHead + 1); // StoreStore memory barrier
         
         return ret;
      }
   }

   public void testCachedHeadTail() throws Throwable
   {
      runTests(new OneToOneQueueCachedHeadTail<Long>(queueSize),"Cached Head/Tail");
   }

   //=====================================================================================
   
   public static final class OneToOneQueue2CachedHeadTail<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      private final AtomicLong tail = new PaddedAtomicLong(0);
      private final AtomicLong head = new PaddedAtomicLong(0);
      
      private long tailCache = 0; // used in poll
      private long headCache = 0; // used in offer
      
      private long offerSideTailCache = 0;
      private long pollSideHeadCache = 0;

      private final int bufferSize;
      private final int indexMask;
      
      @SuppressWarnings("unchecked")
      public OneToOneQueue2CachedHeadTail(int size)
      {
         if (Integer.bitCount(size) != 1)
            throw new IllegalArgumentException("bufferSize must be a power of 2");
         
         this.bufferSize = size;
         this.indexMask = size - 1;

         buffer = (E[])(new Object[size]);
      }
      
      public final boolean offer(final E o)
      {
         final long curTail = offerSideTailCache;
         final long wrapPoint = curTail - bufferSize;
         
         if (headCache <= wrapPoint)
         {
            headCache = head.get();
            if (headCache <= wrapPoint)
               return false;
         }
         
         buffer[(int)(curTail & indexMask)] = o;
         
         offerSideTailCache = curTail + 1;
         tail.lazySet(offerSideTailCache); // StoreStore memory barrier
         
         return true;
      }
      
      public final E poll()
      {
         final long curHead = pollSideHeadCache;
         
         if (curHead >= tailCache)
         {
            tailCache = tail.get();
            if (curHead >= tailCache)
               return null;
         }
         
         final int index = (int)(curHead & indexMask);
         final E ret = buffer[index];
         
         pollSideHeadCache = curHead + 1;
         head.lazySet(pollSideHeadCache); // StoreStore memory barrier
         
         return ret;
      }
   }

   public void test2CachedHeadTail() throws Throwable
   {
      runTests(new OneToOneQueue2CachedHeadTail<Long>(queueSize),"2Cached Head/Tail");
   }

   //=====================================================================================

   public static final class PaddedAtomicLong extends AtomicLong
   {
      private static final long serialVersionUID = 1L;
      
      public volatile long p1,p2,p3,p4,p5,p6 = 7L;

      public long sumPaddingToPreventOptimisation()
      {
          return p1 + p2 + p3 + p4 + p5 + p6;
      }
      
      public PaddedAtomicLong(long v) { super(v); }
   }
   
   public static final class PaddedLong
   {
      public long value = 0L, p1, p2, p3, p4, p5, p6 = 7L;
      
      public long sumPaddingToPreventOptimisation()
      {
          return value + p1 + p2 + p3 + p4 + p5 + p6;
      }
   }
   
   public static final class OneToOneQueuePadded<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      public volatile long value = 0L, p1, p2, p3, p4, p5, p6 = 7L;
      
      public long sumPaddingToPreventOptimisation()
      {
          return value + p1 + p2 + p3 + p4 + p5 + p6;
      }

      private final PaddedAtomicLong tail = new PaddedAtomicLong(0);
      private final PaddedAtomicLong head = new PaddedAtomicLong(0);
      
      private final PaddedLong tailCache = new PaddedLong(); // used in poll
      private final PaddedLong headCache = new PaddedLong(); // used in offer

      private final int bufferSize;
      private final int indexMask;
      
      @SuppressWarnings("unchecked")
      public OneToOneQueuePadded(int size)
      {
         if (Integer.bitCount(size) != 1)
            throw new IllegalArgumentException("bufferSize must be a power of 2");
         
         this.bufferSize = size;
         this.indexMask = size - 1;

         buffer = (E[])(new Object[size]);
      }
      
      public final boolean offer(final E o)
      {
         final long curTail = tail.get();
         final long wrapPoint = curTail - bufferSize;
         if (headCache.value <= wrapPoint)
         {
            headCache.value = head.get();
            if (headCache.value <= wrapPoint)
               return false;
         }
         
         buffer[(int)(curTail & indexMask)] = o;
         
         tail.lazySet(curTail + 1); // StoreStore memory barrier
         
         return true;
      }
      
      public final E poll()
      {
         final long curHead = head.get();
         
         if (curHead >= tailCache.value)
         {
            tailCache.value = tail.get();
            if (curHead >= tailCache.value)
               return null;
         }
         
         final int index = (int)(curHead & indexMask);
         final E ret = buffer[index];
         
         head.lazySet(curHead + 1); // StoreStore memory barrier
         
         return ret;
      }
   }

   public void testPadded() throws Throwable
   {
      runTests(new OneToOneQueuePadded<Long>(queueSize),"Padded");
   }

   //=====================================================================================
   
   public static final class OneToOneQueue2Padded<E> implements SimpleQueue<E>
   {
      private final E[] buffer;
      
      private final PaddedAtomicLong tail = new PaddedAtomicLong(0);
      private final PaddedAtomicLong head = new PaddedAtomicLong(0);
      
      private final PaddedLong tailCache = new PaddedLong(); // used in poll
      private final PaddedLong headCache = new PaddedLong(); // used in offer
      
      private final PaddedLong offerSideTailCache = new PaddedLong();
      private final PaddedLong pollSideHeadCache = new PaddedLong();

      private final int bufferSize;
      private final int indexMask;
      
      @SuppressWarnings("unchecked")
      public OneToOneQueue2Padded(int size)
      {
         if (Integer.bitCount(size) != 1)
            throw new IllegalArgumentException("bufferSize must be a power of 2");
         
         this.bufferSize = size;
         this.indexMask = size - 1;

         buffer = (E[])(new Object[size]);
      }
      
      public final boolean offer(final E o)
      {
         final long curTail = offerSideTailCache.value;
         final long wrapPoint = curTail - bufferSize;
         
         if (headCache.value <= wrapPoint)
         {
            headCache.value = head.get();
            if (headCache.value <= wrapPoint)
               return false;
         }
         
         buffer[(int)(curTail & indexMask)] = o;
         
         final long nextTail = curTail + 1;
         offerSideTailCache.value = nextTail;
         tail.lazySet(nextTail); // StoreStore memory barrier
         
         return true;
      }
      
      public final E poll()
      {
         final long curHead = pollSideHeadCache.value;
         
         if (curHead >= tailCache.value)
         {
            tailCache.value = tail.get();
            if (curHead >= tailCache.value)
               return null;
         }
         
         final int index = (int)(curHead & indexMask);
         final E ret = buffer[index];

         final long nextHead = curHead + 1;
         pollSideHeadCache.value = nextHead;
         head.lazySet(nextHead); // StoreStore memory barrier
         
         return ret;
      }
   }

   public void test2Padded() throws Throwable
   {
      runTests(new OneToOneQueue2Padded<Long>(queueSize),"Padded 2Cached Head/Tail");
   }

   public static void main(String[] args) throws Throwable
   {
      if (args.length > 0)
      {
         MechanicalSympathy o = new MechanicalSympathy();
         for (String arg : args)
         {
            switch(Integer.parseInt(arg))
            {
               case 1:
                  o.testBlockingQueue();
                  break;
               case 2:
                  o.testSynchronized();
                  break;
               case 3:
                  o.testVolatile();
                  break;
               case 4:
                  o.testSoftMB();
                  break;
               case 5:
                  o.testSoftMBMod();
                  break;
               case 6:
                  o.testCachedHeadTail();
                  break;
               case 7:
                  o.test2CachedHeadTail();
                  break;
               case 8:
                  o.testPadded();
                  break;
               case 9:
                  o.test2Padded();
                  break;
               default:
                  usage();   
                  
            }
         }
      }
      else
         usage();
   }
   
   public static void usage()
   {
      System.out.println("usage: java " + MechanicalSympathy.class.getSimpleName() + "0-9");
      System.out.println("        1: ArrayBlockingQueue");
      System.out.println("        2: Synchronized offer/poll");
      System.out.println("        3: Volatile head/tail");
      System.out.println("        4: Soft Memory Barrier");
      System.out.println("        5: Soft Memory Barrier with mod operation");
      System.out.println("        6: Cached Head and Tail.");
      System.out.println("        7: Double Cached Head and Tail.");
      System.out.println("        8: Padded and Cached for false cache sharing.");
      System.out.println("        9: Padded and Double Cached for false cache sharing.");
   }
}
