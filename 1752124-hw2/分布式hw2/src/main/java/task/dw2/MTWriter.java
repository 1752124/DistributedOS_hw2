package task.dw2;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MTWriter {

    private final static int THREAD_NUM = 8;

    public static void main(String[] args) throws Exception{
        CountDownLatch latch = new CountDownLatch(THREAD_NUM);
        long start = System.currentTimeMillis();
        Producer.produceNum();
        File file = new File("C:\\doc\\dw\\wt_nio.txt");
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_NUM);
        int newArrSize = Producer.NUM_ARR.length / THREAD_NUM;
        // 方法一：outputStream方式
//        for (int i = 0; i < THREAD_NUM; i++) {
//            int[] numArr = Arrays.copyOfRange(Producer.NUM_ARR,i*newArrSize,(i+1)*newArrSize);
//            streamExecutorService ws = new streamExecutorService(numArr,file, latch);
//            pool.execute(ws);
//        }
        // 方法二：objectStream方式
//        for (int i = 0; i < THREAD_NUM; i++) {
//            int[] numArr = Arrays.copyOfRange(Producer.NUM_ARR,i*newArrSize,(i+1)*newArrSize);
//            ObjectExecutorService ws = new ObjectExecutorService(numArr,file, latch);
//            pool.execute(ws);
//        }
        // 方法三：nio方式
        for (int i = 0; i < THREAD_NUM; i++) {
            int[] numArr = Arrays.copyOfRange(Producer.NUM_ARR,i*newArrSize,(i+1)*newArrSize);
            NIOExecutorService ws = new NIOExecutorService(numArr,file, latch);
            pool.execute(ws);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("write end, use time is: " + (end - start) + "ms");
    }

}


/**
 * 字节流方式写入
 */
class streamExecutorService implements Runnable {

    private int[] numArr;
    private File file;
    private CountDownLatch latch;

    public streamExecutorService(int[] numArr, File file, CountDownLatch latch) {
        this.numArr = numArr;
        this.file = file;
        this.latch = latch;
    }

    @Override
    public void run() {
        DataOutputStream dos = null;
        try {
            dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, true), 1024*1024));
            for (int one:numArr)
            {
                dos.writeInt(one);
                if (one % 100000 == 0) {
                    System.out.println(Thread.currentThread().getName() + "\t" + one);
                }
            }
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }
    }
}

/**
 * object方式
 */
class ObjectExecutorService implements Runnable {
    private int[] numArr;
    private File file;
    private CountDownLatch latch;

    public ObjectExecutorService(int[] numArr, File file, CountDownLatch latch) {
        this.numArr = numArr;
        this.file = file;
        this.latch = latch;
    }

    @Override
    public void run() {
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file, true),1024*1024));
            out.writeObject(numArr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }
    }
}

/**
 * nio方式
 */
class NIOExecutorService implements Runnable {
    private int[] numArr;
    private File file;
    private CountDownLatch latch;

    public NIOExecutorService(int[] numArr, File file, CountDownLatch latch) {
        this.numArr = numArr;
        this.file = file;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
            FileChannel channel = new RandomAccessFile(file,"rw").getChannel();
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, file.length(), 4 * numArr.length);
            for (int one: numArr) {
                buffer.putInt(one);
            }
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            latch.countDown();
        }
    }
}



