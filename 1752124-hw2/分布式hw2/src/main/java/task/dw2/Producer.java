package task.dw2;

public class Producer {

    // 2014*512*256
    private static final int ARR_SIZE = 263979008;
//    private static final int ARR_SIZE = 2014*512;

    // 2014*512
    public static final int MAX_NUM = 1031168;

//     重复次数
    private static final int REPEAT_NUM = 256;
//    private static final int REPEAT_NUM = 1;

    public static int[] NUM_ARR = new int[ARR_SIZE];

    public static void produceNum() {
        int idx = 0;
        for (int i = 0; i < MAX_NUM; i++) {
            for (int j = 0; j < REPEAT_NUM; j++) {
                NUM_ARR[idx++] = i+1;
            }
        }
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        produceNum();
        long end = System.currentTimeMillis();
        System.out.println("use time is: " + (end - start) + "ms");
        System.out.println(NUM_ARR[ARR_SIZE-1]);
    }
}
