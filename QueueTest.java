import java.util.*;
import java.io.*;
import java.net.*;

import okuyama.imdst.util.*;
import okuyama.imdst.client.OkuyamaQueueClient  ;
import okuyama.base.lang.BatchException;

public class QueueTest {

    public static void main(String[] args) {
        try {
            String methodType = args[0];
            String masterNodeInfos = args[1];

            OkuyamaQueueClient queueClient = new OkuyamaQueueClient();
            queueClient.setConnectionInfos(masterNodeInfos.split(","));
            queueClient.autoConnect();

            if (methodType.equals("create")) {

                if(queueClient.createQueueSpace(args[2])) {
                    System.out.println("Create Queue Success QueueName=[" + args[2] + "]");
                } else {
                    System.out.println("Create Queue Error QueueName=[" + args[2] + "]");
                }
            } else if (methodType.equals("put")) {

                if(queueClient.put(args[2], args[3])) {
                    System.out.println("Put Queue Success QueueName=[" + args[2] + "] Data=[" + args[3] + "]");
                } else {
                    System.out.println("Put Queue Error QueueName=[" + args[2] + "] Data=[" + args[3] + "]");
                }
            } else if (methodType.equals("putmany")) {

                for (int idx = 0; idx < Integer.parseInt(args[4]); idx++) {
                    if(queueClient.put(args[2], args[3] + "_" + idx)) {
                        if ((idx % 1000) == 0) System.out.println("Put Queue Count=[" + idx + "]");
                    } else {
                        System.out.println("Put Queue Error QueueName=[" + args[2] + "] Data=[" + args[3] + "_" + idx + "]");
                    }
                }
            } else if (methodType.equals("take")) {

                String data = null;
                data = queueClient.take(args[2], 10000);
                if(data != null) {
                    System.out.println("Take Queue Success QueueName=[" + args[2] + "] Data=[" + data + "]");
                } else {
                    System.out.println("Take Queue Error QueueName=[" + args[2] + "] Data=[" + data + "]");
                }
            } else if (methodType.equals("takemany")) {

                String data = null;
                while (true) {

                    data = queueClient.take(args[2], 10000);
                    if(data != null) {
                        System.out.println("Take Queue Success QueueName=[" + args[2] + "] Data=[" + data + "]");
                    } else {
                        break;
                    }
                }
            } else if (methodType.equals("remove")) {

                if(queueClient.removeQueueSpace(args[2])) {
                    System.out.println("Remove Queue Success QueueName=[" + args[2] + "]");
                } else {
                    System.out.println("Remove Queue Error QueueName=[" + args[2] + "]");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
