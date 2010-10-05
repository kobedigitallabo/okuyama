import java.util.*;
import java.io.*;

import java.util.concurrent.locks.*;
import java.util.*;


public class Test2 {

    public static void main(String[] args) {
        try {
            String[] dirs = {"./data/data1/","./data/data2/","./data/data3/","./data/data4/"};
            FileBaseDataMap fileBaseDataMap = new FileBaseDataMap(dirs, 4000);
long start = System.nanoTime();
long start2 = 0L;
            for (int idx = 0; idx < 4000; idx++) {
                fileBaseDataMap.put("key-abcdefghijklmnopqrstuwxyz0123456789-" + (idx + 100000), new Integer(idx).toString());
                if ((idx % 10000) == 0){
                    long end2 = System.nanoTime();
                    System.out.println(idx + "=" + ((end2 - start2) / 1000 / 1000));
                    start2 = System.nanoTime();
                }
            }
long end = System.nanoTime();
System.out.println("Total Write Time =" + ((end - start) / 1000 / 1000));



start = System.nanoTime();
start2 = 0L;
            for (int idx = 0; idx < 4000; idx++) {
                //System.out.println(fileBaseDataMap.get("key-abcdefghijklmnopqrstuwxyz0123456789-" + (idx + 100000)));
                if ((idx % 10000) == 0){
                    long end2 = System.nanoTime();
                    System.out.println(idx + "=" + ((end2 - start2) / 1000 / 1000));
                    start2 = System.nanoTime();
                }
            }
end = System.nanoTime();
System.out.println("Total Read Time =" + ((end - start) / 1000 / 1000));



start = System.nanoTime();
start2 = 0L;
            for (int idx = 2500; idx < 4000; idx++) {
                fileBaseDataMap.put("key-abcdefghijklmnopqrstuwxyz0123456789-" + (idx + 100000), new Integer(idx).toString());
                if ((idx % 10000) == 0){
                    long end2 = System.nanoTime();
                    System.out.println(idx + "=" + ((end2 - start2) / 1000 / 1000));
                    start2 = System.nanoTime();
                }
            }
end = System.nanoTime();
System.out.println("Total Write Time =" + ((end - start) / 1000 / 1000));



            Set entrySet = null;
            Iterator entryIte = null;
            String key = null;

            entrySet = fileBaseDataMap.entrySet();
            entryIte = entrySet.iterator();

            int varIdx = 0;
            while(entryIte.hasNext()) {
                Map.Entry obj = (Map.Entry)entryIte.next();
                key = (String)obj.getKey();
//                System.out.println(key);
                System.out.println(obj.getValue());
                varIdx++;
            }
System.out.println(varIdx);


            System.out.println(fileBaseDataMap.size());
        } catch (Exception e) {
            e.printStackTrace();
        } 
    }
}
