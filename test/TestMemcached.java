package test;
import com.danga.MemCached.*;
import java.util.*;
import java.util.Date;
import java.util.Calendar;

public class TestMemcached {
    public static void main(String[] args) {
        try {
            System.out.println("args[0]=server:prot, args[1]=execcount");
            String[] serverlist = {args[0]};
            int count = Integer.parseInt(args[1]);
            // initialize the pool for memcache servers
            SockIOPool pool = SockIOPool.getInstance();
            pool.setServers( serverlist );

            pool.initialize();

            MemCachedClient mc = new MemCachedClient();
            mc.setCompressEnable( false );
            mc.setPrimitiveAsString( true );

            for (int exec = 0; exec < count; exec++) {
                String prefix = "Prefix-" + ((exec++) / 2) + "_";

                mc.set(prefix+ "key111",  0);
                mc.set(prefix+ "key222",  0);
                mc.set(prefix+ "key333",  "abc");
                mc.set(prefix+ "key444",  "def");
                System.out.println("==============");
                System.out.println(mc.incr(prefix+ "key111", 1));
                System.out.println(mc.incr(prefix+ "key111", 10));
                System.out.println(mc.incr(prefix+ "key111", 100));

                System.out.println(mc.decr(prefix+ "key222", 1));
                System.out.println(mc.decr(prefix+ "key222", 10));
                System.out.println(mc.decr(prefix+ "key222", 100));

                System.out.println(mc.incr(prefix+ "key333", 10));
                System.out.println(mc.decr(prefix+ "key444", 10));

                System.out.println("==============");



                String[] testKeys = {prefix+ "key111", prefix+ "key222", prefix+ "key333", prefix+ "keyXXX", prefix+ "key444"};
                Map a = (Map)mc.getMulti(testKeys);
                System.out.println(a);

                // 100KBを超える大きなデータ
                String bigValStr = "";
                Random rnd = new Random();
                for (int i = 0; i < 10000; i++) {
                    bigValStr = bigValStr + rnd.nextInt(1999999999);
                }

                bigValStr = bigValStr + "=";

                String bigValStr2 = "DataValueStrXXXX2X1XX8888123456yhtgrfedwsqaXXXXXX3X5XXXX8888123456yhtgrfedwsqaXYYY8888123456yhtgrfedwsqaYYYY245YYYYZZZ88488123456yhtgrfedwsqaZZZZ53ZZZZ!gr!!!!!!!!2222222ghtf2222gf22223r33333g333344444444re4f555555555555rt5566666gr66666788881gr23456yhtgrfedwsqa777777ads7777787ga8888888ga8123456yhtgrfedwsqa8888888afd88888889a0fd0saf0000088ad8f8123456yhtgrfedwsqa0000098=";

                for (int idx = 0; idx < 2500; idx++) {

                    Object val = mc.get(prefix+ "key_memcached_XXX=" + idx);


                    if (val == null || !val.equals(bigValStr + idx)) {

                        //System.out.println("Get[1] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + val + "]");
                    }
                    if ((idx % 500) == 0) System.out.println(idx);
                }

                System.out.println("TestGet Finish");

                for (int idx = 0; idx < 2500; idx++) {

                    if(!mc.set(prefix+ "key_memcached_XXX=" + idx, bigValStr +idx)) {
                        System.out.println("Set[1] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + idx +"]");
                    }
                }
                System.out.println("Set[1]=finish");

                for (int idx = 0; idx < 2500; idx++) {

                    Object val = mc.get(prefix+ "key_memcached_XXX=" + idx);
                    if (val == null || !val.equals(bigValStr +idx)) {
                        System.out.println("Get[1] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + val + "]");
                    }
                }
                System.out.println("Get[1]=finish");


                for (int idx = 2500; idx < 20000; idx++) {

                    if(!mc.set(prefix+ "key_memcached_XXX=" + idx, bigValStr2 +idx)) {
                        System.out.println("Set[1] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + idx +"]");
                    }
                }
                System.out.println("Set[1-1]=finish");

                for (int idx =2500; idx < 20000; idx++) {

                    Object val = mc.get(prefix+ "key_memcached_XXX=" + idx);
                    if (val == null || !val.equals(bigValStr2 +idx)) {
                        System.out.println("Get[1] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + val + "]");
                    }
                }
                System.out.println("Get[1-1]=finish");

                for (int idx = 2500; idx < 20000; idx++) {

                    if(!mc.set(prefix+ "key_memcached_XXX=" + idx, "ValueMemcached-TestValueXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZ9876543210-XXXXXXXXXXXXXXX="+idx)) {
                        System.out.println("Set[2] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + "ValueMemcached-TestValueXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZ9876543210-XXXXXXXXXXXXXXX="+ idx +"]");
                    }
                }
                System.out.println("Set[2]=finish");

                for (int idx = 2500; idx < 20000; idx++) {

                    Object val = mc.get(prefix+ "key_memcached_XXX=" + idx); 
                    if (val == null || !val.equals("ValueMemcached-TestValueXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZXXXXXXXXXXYYYYYYYYYYZZZZZZZZZZ9876543210-XXXXXXXXXXXXXXX="+idx)) {
                        System.out.println("Get[2] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + val + "]");
                    }
                }
                System.out.println("Get[2]=finish");

                for (int idx = 0; idx < 5000; idx++) {

                    if(!mc.delete(prefix+ "key_memcached_XXX=" + idx)) {
                        System.out.println("Delete[1] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "]");
                    }
                }
                System.out.println("Delete[1]=finish");

                for (int idx = 0; idx < 5000; idx++) {

                    Object val = mc.get(prefix+ "key_memcached_XXX=" + idx) ;
                    if (val != null) {
                        System.out.println("Get[3] - Error Key=[" + prefix+ "key_memcached_XXX=" + idx + "] Value=[" + val + "]");
                    }
                }
                System.out.println("Get[3]=finish");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}