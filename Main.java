import java.util.*;
import java.io.*;

public class Main extends Thread {

    public volatile int name = -1;

    public static int dataSize = 1000000;

    public static String[] fileDirs = {"K:/work/data/data1/","K:/work/data/data2/","K:/work/data/data3/","K:/work/data/data4/", "C:/desktop/tools/java/FileKeyValueMap/data/data1/", "C:/desktop/tools/java/FileKeyValueMap/data/data2/", "C:/desktop/tools/java/FileKeyValueMap/data/data3/", "C:/desktop/tools/java/FileKeyValueMap/data/data4/"};
    //public static String[] fileDirs = {"K:/work/data/", "C:/desktop/tools/java/FileKeyValueMap/data/", "J:/work/data/"};


    public static FileHashMap fileHashMap = new FileHashMap();

    public static void main(String[] args) {
        Main[] me = new Main[Integer.parseInt(args[0])];

        try {
            long start = System.nanoTime();
            System.out.println(fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + "0" + "_" + 15483));
            System.out.println(fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + "1" + "_" + 45896));
            long end = System.nanoTime();
            System.out.println((end - start));

            long start2 = System.nanoTime();
            
            fileHashMap.put("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_1_177999", "adfadfsdafqwertqwfqrytreuhtyri ghngrfcqqedxew");
            long end2 = System.nanoTime();
            System.out.println((end2 - start2));

            for (int i = 0; i < Integer.parseInt(args[0]); i++) {
                me[i] = new Main(i);
            }

            for (int i = 0; i < Integer.parseInt(args[0]); i++) {
                me[i].start();
            }

            for (int i = 0; i < Integer.parseInt(args[0]); i++) {
                me[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Main(int setName) {
        this.name = setName;
    }

    public void run() {
        exec();
    }

    public void exec() {

        long start1 = 0L;
        long end1 = 0L;
        Random rdn = new Random();
        long start = System.currentTimeMillis();
        //for (int i = 0; i < dataSize; i++) {
        for (int i = 0; i < 20000; i++) {
            fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + i);
            //fileHashMap.get("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + rdn.nextInt(10000));
        }
        long end = System.currentTimeMillis();
        System.out.println("Data Read Time = [" + (end - start) + "]");

        start = System.currentTimeMillis();
        for (int i = 0; i < dataSize; i++) {
            fileHashMap.put("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + i, "valueAbCddEfGhIjK`;:p;:pp;:ppo8547asdfasdfasfasfl@lkwoqihfriueqnwqerq09876sdfdqweih7822kuioZj_20100101235959999_" + this.name + i);
            //fileHashMap.put("keyAbCddEfGhIjK`;:p;:pp;:ppo8547asdf7822kuioZj_20100101235959999_" + this.name + "_" + i, "valueAbCddEfGhIjK`;:p;:pp;:ppo8547asdfasdfasfasfl@lkwoqihfriueqnwqerq09876sdfdqweih7822kuioZj_20100101235959999_" + this.name + rdn.nextInt(10000));
            if ((i % 10000) == 0) {
                end1 = System.currentTimeMillis();
                System.out.println(i + "=" + (end1 - start1));
                start1 = System.currentTimeMillis();
            }
        }
        end = System.currentTimeMillis();
        System.out.println((end - start));
        System.out.println("Data Write Time = [" + (end - start) + "]");
        System.out.println("----------------------------------------");

    }
}

class FileHashMap {

    int keyDataLength = 256;

    int oneDataLength = 1791;

    int lineDataSizeNoRt =  keyDataLength + oneDataLength;
    
    int lineDataSize =  keyDataLength + oneDataLength + 1;

    // 一度に取得するデータサイズ
    int getDataSize = lineDataSize * 16;

    int accessCount = 2048;

    Object[] fileAccessList = new Object[accessCount];

    public FileHashMap() {
        try {
            for (int i = 0; i < accessCount; i++) {
                File file = new File(Main.fileDirs[i % Main.fileDirs.length] + i + ".data");

                BufferedWriter wr = new BufferedWriter(new FileWriter(file, true));

                RandomAccessFile raf = new RandomAccessFile(file, "rwd");
                Object[] accessors = new Object[3];
                accessors[0] = file;
                accessors[1] = raf;
                accessors[2] = wr;
                fileAccessList[i] = accessors;
            }
            System.out.println("Start OK");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void put(String key, String value) {
        try {


            int index = key.hashCode();
            if (index < 0) {
                index = index - index - index;
            }

            Object[] accessors = (Object[])fileAccessList[index % accessCount];

            RandomAccessFile raf = (RandomAccessFile)accessors[1];

            BufferedWriter wr = (BufferedWriter)accessors[2];

            StringBuffer buf = new StringBuffer(this.fillCharacter(key, keyDataLength));
            buf.append(this.fillCharacter(value, oneDataLength));
            buf.append("\n");

            synchronized (raf) {
                long dataLineNo = this.getLinePoint(key);

                if (dataLineNo == -1) {
                    wr.write(buf.toString());
                    wr.flush();
                } else {
                    raf.seek(dataLineNo * (lineDataSize));
                    raf.write(buf.toString().getBytes(), 0, oneDataLength);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


/*
    // スレッドセーフではない
    public long getLinePoint(String key) {

        long line = -1;
        long lineCount = 0L;

        byte[] keyBytes = key.getBytes();
        byte[] equalKeyBytes = new byte[keyBytes.length + 1];

        // マッチング用配列作成
        for (int idx = 0; idx < keyBytes.length; idx++) {
            equalKeyBytes[idx] = keyBytes[idx];
        }

        equalKeyBytes[equalKeyBytes.length - 1] = 38;

        try {
            int index = key.hashCode();
            if (index < 0) {
                index = index - index - index;
            }

            Object[] accessors = (Object[])fileAccessList[index % accessCount];

            RandomAccessFile raf = (RandomAccessFile)accessors[1];

            raf.seek(0);

            byte[] lineBuf = new byte[lineDataSize];
            boolean matchFlg = true;

            while(raf.read(lineBuf) != -1) {

                matchFlg = true;

                for (int i = 0; i < equalKeyBytes.length; i++) {

                    if (equalKeyBytes[i] != lineBuf[i]) {
                        matchFlg = false;
                    }
                }

                // マッチした場合のみ文字列化
                if (matchFlg) {
                    line = lineCount;
                    break;
                }

                lineCount++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return line;
    }*/


    public long getLinePoint(String key) {

        long line = -1;
        long lineCount = 0L;


        String ret = null;
        byte[] keyBytes = key.getBytes();
        byte[] equalKeyBytes = new byte[keyBytes.length + 1];
        byte[] lineBufs = new byte[this.getDataSize];
        boolean matchFlg = true;

        // マッチング用配列作成
        for (int idx = 0; idx < keyBytes.length; idx++) {
            equalKeyBytes[idx] = keyBytes[idx];
        }

        equalKeyBytes[equalKeyBytes.length - 1] = 38;

        try {
            int index = key.hashCode();
            if (index < 0) {
                index = index - index - index;
            }

            Object[] accessors = (Object[])fileAccessList[index % accessCount];

            RandomAccessFile raf = (RandomAccessFile)accessors[1];

            raf.seek(0);
            int readLen = -1;
            while((readLen = raf.read(lineBufs)) != -1) {

                matchFlg = true;

                int loop = readLen / lineDataSize;

                for (int loopIdx = 0; loopIdx < loop; loopIdx++) {

                    int assist = (lineDataSize * loopIdx);

                    matchFlg = true;

                    for (int i = 0; i < equalKeyBytes.length; i++) {
                        lineCount++;
                        if (equalKeyBytes[i] != lineBufs[assist + i]) {
                            matchFlg = false;
                            break;
                        }
                    }

                    // マッチした場合のみ文字列化
                    if (matchFlg) {
                        line = lineCount;
                        break;
                    }
                }
                if (matchFlg) break;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return line;
    }




    public String get(String key) {
        String tmpValue = null;

        String ret = null;
        byte[] keyBytes = key.getBytes();
        byte[] equalKeyBytes = new byte[keyBytes.length + 1];
        byte[] lineBufs = new byte[this.getDataSize];
        boolean matchFlg = true;

        // マッチング用配列作成
        for (int idx = 0; idx < keyBytes.length; idx++) {
            equalKeyBytes[idx] = keyBytes[idx];
        }

        equalKeyBytes[equalKeyBytes.length - 1] = 38;

        try {
            int index = key.hashCode();
            if (index < 0) {
                index = index - index - index;
            }

            Object[] accessors = (Object[])fileAccessList[index % accessCount];

            RandomAccessFile raf = (RandomAccessFile)accessors[1];

            synchronized (raf) {
                raf.seek(0);
                int readLen = -1;
                while((readLen = raf.read(lineBufs)) != -1) {

                    matchFlg = true;

                    int loop = readLen / lineDataSize;

                    for (int loopIdx = 0; loopIdx < loop; loopIdx++) {

                        int assist = (lineDataSize * loopIdx);

                        matchFlg = true;

                        for (int i = 0; i < equalKeyBytes.length; i++) {

                            if (equalKeyBytes[i] != lineBufs[assist + i]) {
                                matchFlg = false;
                                break;
                            }
                        }

                        // マッチした場合のみ文字列化
                        if (matchFlg) {
                            tmpValue = new String(lineBufs, assist, lineDataSizeNoRt);
                            break;
                        }
                    }
                    if (matchFlg) break;
                }
            }

            if (tmpValue != null) {
                byte[] workBytes = tmpValue.getBytes();

                if (workBytes[keyDataLength] != 38) {
                    int i = keyDataLength;
                    int counter = 0;
                    for (; i < workBytes.length; i++) {
                        if (workBytes[i] == 38) break;
                        counter++;
                    }

                    ret = new String(workBytes, keyDataLength, counter, "UTF-8");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    private String fillCharacter(String data, int fixSize) {
        StringBuffer writeBuf = new StringBuffer(data);

        int valueSize = data.length();

        // 渡されたデータが固定の長さ分ない場合は足りない部分を補う
        // 足りない文字列は固定の"&"で補う(38)
        byte[] appendDatas = new byte[fixSize - valueSize];

        for (int i = 0; i < appendDatas.length; i++) {
            appendDatas[i] = 38;
        }

        writeBuf.append(new String(appendDatas));
        return writeBuf.toString();
    }
}