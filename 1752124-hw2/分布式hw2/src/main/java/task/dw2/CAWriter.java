package task.dw2;

import com.datastax.driver.core.*;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Cassandra
 */
public class CAWriter {

    private final static int THREAD_NUM = 1;

    private final static String TABLE_NAME = "dw_1_b";

    private Cluster cluster;
    private Session session;

    public static void main(String[] args) throws Exception{
        CountDownLatch latch = new CountDownLatch(THREAD_NUM);
        long start = System.currentTimeMillis();
        CAWriter caWriter = new CAWriter();
        // 建立连接
        caWriter.connect();
        caWriter.createTable();
        Producer.produceNum();
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_NUM);
        int newArrSize = Producer.NUM_ARR.length / THREAD_NUM;
        // 方法一：单个插入
//        for (int i = 0; i < THREAD_NUM; i++) {
//            int[] numArr = Arrays.copyOfRange(Producer.NUM_ARR,i*newArrSize,(i+1)*newArrSize);
//            CAExecutor ws = new CAExecutor(numArr,caWriter.session, latch);
//            pool.execute(ws);
//        }
        // 方法二：批量插入
        for (int i = 0; i < THREAD_NUM; i++) {
            int[] numArr = Arrays.copyOfRange(Producer.NUM_ARR,i*newArrSize,(i+1)*newArrSize);
            BatchExecutor ws = new BatchExecutor(numArr,caWriter.session, latch);
            pool.execute(ws);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("ca write end, use time is: " + (end - start) + "ms");
//        caWriter.query();
    }

    /**
     * 建立连接
     */
    private void connect()
    {
        // addContactPoints:cassandra节点ip withPort:cassandra节点端口 默认9042
        cluster = Cluster.builder().addContactPoints("39.97.175.111").withPort(9042)
                .withCredentials("cassandra", "cassandra").build();
        session = cluster.connect();
    }

    /**
     * 创建键空间, 键名mydb
     */
    private void createKeyspace()
    {
        // 单数据中心 复制策略 ：1
        String cql = "CREATE KEYSPACE if not exists mydb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}";
        session.execute(cql);
    }

    /**
     * 创建表, 表名 dw
     */
    private void createTable()
    {
        String cql = "CREATE TABLE if not exists mydb." + TABLE_NAME + " (id text,a int,PRIMARY KEY(id))";
        session.execute(cql);
    }


    /**
     * 查询
     */
    private void query()
    {
        String cql = "SELECT * FROM mydb."+TABLE_NAME+";";

        ResultSet resultSet = session.execute(cql);
        System.out.print("这里是字段名：");
        for (ColumnDefinitions.Definition definition : resultSet.getColumnDefinitions())
        {
            System.out.print(definition.getName() + " ");
        }
        System.out.println();
        System.out.println(String.format("%s\t%s\t", "id","a"));
        System.out.println("==========================");
        for (Row row : resultSet)
        {
            System.out.println(String.format("%s\t%d\t", row.getString("id"), row.getInt("a")));
        }
    }
}

/**
 * 插入执行器
 */
class CAExecutor implements Runnable {
    private int[] numArr;
    private CountDownLatch latch;
    private Session session;

    public CAExecutor(int[] numArr, Session session, CountDownLatch latch) {
        this.numArr = numArr;
        this.session = session;
        this.latch = latch;
    }

    @Override
    public void run() {
        for (int one: numArr) {
            String uuid = UUID.randomUUID().toString().replaceAll("-","");
            String cql = String.format("INSERT INTO mydb.%s (id,a) VALUES (%s,%d);", uuid,one);
            session.execute(cql);
        }
        latch.countDown();
    }
}

/**
 * 批量插入执行器
 */
class BatchExecutor implements Runnable {
    private int[] numArr;
    private CountDownLatch latch;
    private Session session;

    public BatchExecutor(int[] numArr, Session session, CountDownLatch latch) {
        this.numArr = numArr;
        this.session = session;
        this.latch = latch;
    }

    @Override
    public void run() {
        BatchStatement batch = new BatchStatement();
        PreparedStatement ps = session.prepare("insert into mydb.dw(id,a) values(?,?)");
        for (int i = 0; i < numArr.length; i++) {
            String uuid = UUID.randomUUID().toString().replaceAll("-","");
            BoundStatement bs = ps.bind(uuid,numArr[i]);
            batch.add(bs);
            if ((i+1) % 1024 == 0) {
                session.execute(batch);
                batch.clear();
                batch = new BatchStatement();
            }
        }
        session.execute(batch);
        batch.clear();
        latch.countDown();
    }
}
