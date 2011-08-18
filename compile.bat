rmdir /Q /S classes
mkdir classes
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/base/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/util/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/util/io/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/job/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/helper/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/client/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/client/io/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/process/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/util/serializemap/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/jetty-6.1.0.jar;./lib/jetty-util-6.1.0.jar;./lib/servlet-api-2.5.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/manager/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/jetty-6.1.0.jar;./lib/jetty-util-6.1.0.jar;./lib/servlet-api-2.5.jar -encoding utf-8 -d ./classes ./src/okuyama/imdst/manager/servlet/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/jetty-6.1.0.jar;./lib/jetty-util-6.1.0.jar;./lib/servlet-api-2.5.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/test/job/*.java
javac -cp ./src;./lib/log4j-1.2.14.jar;./lib/javamail-1.4.1.jar;./lib/jetty-6.1.0.jar;./lib/jetty-util-6.1.0.jar;./lib/servlet-api-2.5.jar;./lib/commons-codec-1.4.jar -encoding utf-8 -d ./classes ./src/test/helper/*.java
xcopy /Q /F src\*.properties classes
