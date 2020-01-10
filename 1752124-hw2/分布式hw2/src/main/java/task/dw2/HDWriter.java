package task.dw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HDWriter {

    private final static int THREAD_NUM = 1;

    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://master:9000");
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("mapred-site.xml");
        FileSystem fs = FileSystem.get(conf);
        return fs;
    }

    public static void main(String[] args) throws Exception{
        CountDownLatch latch = new CountDownLatch(THREAD_NUM);
        long start = System.currentTimeMillis();
        Producer.produceNum();
        String srcFileName = "C:\\doc\\dw\\hd_nio2333.txt";
        String hdfsFileName = "/upload/hd_22.txt";
        File srcFile = new File(srcFileName);
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_NUM);
        int newArrSize = Producer.NUM_ARR.length / THREAD_NUM;
        for (int i = 0; i < THREAD_NUM; i++) {
            int[] numArr = Arrays.copyOfRange(Producer.NUM_ARR,i*newArrSize,(i+1)*newArrSize);
            HDFSExecutor ws = new HDFSExecutor(numArr,srcFile, latch);
            pool.execute(ws);
        }
        FileSystem fs = getFileSystem();
        latch.await();
        // 方式一：本地写文件，然后上传hdfs系统
//        fs.copyFromLocalFile(new Path(srcFileName),new Path(hdfsFileName));
        // 方式二：hdfs写文件
        InputStream in = new FileInputStream(srcFileName);
        OutputStream out = fs.create(new Path(hdfsFileName));
        IOUtils.copyBytes(in, out, 1024*1024,true);
        long end = System.currentTimeMillis();
        System.out.println("write end, use time is: " + (end - start) + "ms");
    }
}



/**
 * 本地写文件
 */
class HDFSExecutor implements Runnable {
    private int[] numArr;
    private File file;
    private CountDownLatch latch;

    public HDFSExecutor(int[] numArr, File file, CountDownLatch latch) {
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
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}