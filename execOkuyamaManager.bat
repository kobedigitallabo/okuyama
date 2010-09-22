rem http://localhost:10088/okuyamamgrでアクセス可能
java -cp .;./classes;./lib/jetty-6.1.0.jar;./lib/jetty-util-6.1.0.jar;./lib/servlet-api-2.5.jar;./lib/javamail-1.4.1.jar -Xmx64m org.okuyama.imdst.manager.OkuyamaManagerServer 10088 localhost:8888
