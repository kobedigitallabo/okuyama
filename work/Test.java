public class Test {
    public static void main(String[] args) {
        String[] dirs  ={"C:/desktop/tools/java/okuyama/trunk/work/data1/","C:/desktop/tools/java/okuyama/trunk/work/data2/", "K:/work/data1/", "K:/work/data2/"};
        FileBaseDataMap fileBaseDataMap = new FileBaseDataMap(dirs);


long start = System.nanoTime();
        for (int idx = 0; idx < 100000; idx++) {

            fileBaseDataMap.put("key" + idx, "value" + idx);
        }
long end = System.nanoTime();
System.out.println((end - start) / 1000 / 1000);

start = System.nanoTime();
        for (int idx = 0; idx < 10000; idx++) {

            fileBaseDataMap.get("key" + idx);
        }
end = System.nanoTime();
System.out.println((end - start) / 1000 / 1000);


start = System.nanoTime();
        for (int idx = 0; idx < 4500; idx++) {

            System.out.println(fileBaseDataMap.remove("key" + idx));
        }
end = System.nanoTime();
System.out.println((end - start) / 1000 / 1000);

    }
}