# Purpose
pure scala version of https://github.com/jgperrin/net.jgp.books.spark.ch14

# Environment
- Java 11
- Scala 2.13.8
- Spark 3.2.1

# How to run
## 1, sbt package, in project root dir
When success, there a jar file at ./target/scala-2.13. The name is `main-scala-ch14_2.13-1.0.jar` (the same as name property in sbt file)


## 2, submit jar file, in project root dir
```
// common jar, need --jars option
$ YOUR_SPARK_HOME/bin/spark-submit \
  --class net.jgp.books.spark.MainApp \
  --master "local[*]" --jars jars/spark-daria_2.13-1.2.3.jar \
  target/scala-2.13/main-scala-ch14_2.13-1.0.jar
```

## 3, print

### Case: udf
```
root
 |-- Council_ID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Opening_Hours_Monday: string (nullable = true)
 |-- Opening_Hours_Tuesday: string (nullable = true)
 |-- Opening_Hours_Wednesday: string (nullable = true)
 |-- Opening_Hours_Thursday: string (nullable = true)
 |-- Opening_Hours_Friday: string (nullable = true)
 |-- Opening_Hours_Saturday: string (nullable = true)

+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+
|Council_ID|Name                                |Opening_Hours_Monday                                              |Opening_Hours_Tuesday                                             |Opening_Hours_Wednesday                                           |Opening_Hours_Thursday                                            |Opening_Hours_Friday                      |Opening_Hours_Saturday|
+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+
|SD1       |County Library                      |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD2       |Ballyroan Library                   |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD3       |Castletymon Library                 |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-17:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD4       |Clondalkin Library                  |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD5       |Lucan Library                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-20:00                                                       |09:45-16:30                               |09:45-16:30           |
|SD6       |Whitechurch Library                 |14:00-17:00 and 18:00-20:00                                       |14:00-17:00 and 18:00-20:00                                       |09:45-13:00 and 14:00-17:00                                       |14:00-17:00 and 18:00-20:00                                       |Closed                                    |Closed                |
|SD7       |The John Jennings Library (Stewarts)|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|10:00-16:00 - closed for lunch 12:30-13:00|Closed                |
+----------+------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------------------------------+------------------------------------------+----------------------+

root
 |-- Council_ID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- date: timestamp (nullable = true)
 |-- open: struct (nullable = true)
 |    |-- _1: string (nullable = true)
 |    |-- _2: integer (nullable = false)
 |    |-- _3: boolean (nullable = false)

root
 |-- Council_ID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- date: timestamp (nullable = true)
 |-- desc: string (nullable = true)
 |-- weekDay: integer (nullable = true)
 |-- opening: boolean (nullable = true)


+----------+------------------------------------+-------------------+------------------------------------------------------------------+-------+-------+
|Council_ID|Name                                |date               |desc                                                              |weekDay|opening|
+----------+------------------------------------+-------------------+------------------------------------------------------------------+-------+-------+
|SD1       |County Library                      |2020-03-02 21:36:19|09:45-20:00                                                       |1      |false  |
|SD2       |Ballyroan Library                   |2020-03-02 21:36:19|09:45-20:00                                                       |1      |false  |
|SD3       |Castletymon Library                 |2020-03-02 21:36:19|09:45-17:00                                                       |1      |false  |
|SD4       |Clondalkin Library                  |2020-03-02 21:36:19|09:45-20:00                                                       |1      |false  |
|SD5       |Lucan Library                       |2020-03-02 21:36:19|09:45-20:00                                                       |1      |false  |
|SD6       |Whitechurch Library                 |2020-03-02 21:36:19|14:00-17:00 and 18:00-20:00                                       |1      |false  |
|SD7       |The John Jennings Library (Stewarts)|2020-07-02 16:36:19|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|4      |false  |
|SD7       |The John Jennings Library (Stewarts)|2020-03-02 21:36:19|10:00-17:00 (16:00 July and August) - closed for lunch 12:30-13:00|1      |false  |
+----------+------------------------------------+-------------------+------------------------------------------------------------------+-------+-------+
```

## 4, Some diffcult case

### Scala process DateTime
Ref: [Joda-Time](https://www.joda.org/joda-time/userguide.html), [nscala-time](https://github.com/nscala-time/nscala-time)

### Java 11 vs Java 8
when I use `new Timestamp(now)` with java 11 throws a exception, but change to Java 8 is fine.
Java 9 also have this problem, [here](https://github.com/com-lihaoyi/mill/pull/255)
```
Welcome to Scala 2.13.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_191).
Type in expressions for evaluation. Or try :help.

scala> import java.sql.Timestamp
import java.sql.Timestamp

scala> val now = System.currentTimeMillis()
val now: Long = 1654745803022

scala> val sqlTimestamp = new Timestamp(now)
val sqlTimestamp: java.sql.Timestamp = 2022-06-09 11:36:43.022

scala> sqlTimestamp.getTime
val res0: Long = 1654745803022

scala> import com.github.nscala_time.time.Imports._
import com.github.nscala_time.time.Imports._

scala> val dt = new DateTime(sqlTimestamp.getTime)
val dt: org.joda.time.DateTime = 2022-06-09T11:36:43.022+08:00

scala> dt.getYear
val res1: Int = 2022
```
Throws Exception.
```
Welcome to Scala 2.13.8 (OpenJDK 64-Bit Server VM, Java 11.0.12).
Type in expressions for evaluation. Or try :help.

scala> import java.sql.Timestamp
import java.sql.Timestamp

scala> val now = System.currentTimeMillis()
val now: Long = 1654746251969

scala> val sqlTimestamp = new Timestamp(now)
java.lang.SecurityException: Prohibited package name: java.sql
  at java.base/java.lang.ClassLoader.preDefineClass(ClassLoader.java:899)
  at java.base/java.lang.ClassLoader.defineClass(ClassLoader.java:1015)
  at java.base/java.security.SecureClassLoader.defineClass(SecureClassLoader.java:174)
  at java.base/java.net.URLClassLoader.defineClass(URLClassLoader.java:550)
  at java.base/java.net.URLClassLoader$1.run(URLClassLoader.java:458)
  at java.base/java.net.URLClassLoader$1.run(URLClassLoader.java:452)
  at java.base/java.security.AccessController.doPrivileged(Native Method)
  at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:451)
  at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:589)
  at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:576)
  at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:522)
  at java.base/java.lang.Class.getDeclaredMethods0(Native Method)
  at java.base/java.lang.Class.privateGetDeclaredMethods(Class.java:3166)
  at java.base/java.lang.Class.privateGetPublicMethods(Class.java:3191)
  at java.base/java.lang.Class.getMethods(Class.java:1904)
  at scala.tools.nsc.interpreter.IMain$ReadEvalPrint.evalMethod(IMain.scala:728)
  at scala.tools.nsc.interpreter.IMain$ReadEvalPrint.call(IMain.scala:665)
  at scala.tools.nsc.interpreter.IMain$Request.loadAndRun(IMain.scala:1020)
  at scala.tools.nsc.interpreter.IMain.$anonfun$doInterpret$1(IMain.scala:506)
  at scala.reflect.internal.util.ScalaClassLoader.asContext(ScalaClassLoader.scala:36)
  at scala.reflect.internal.util.ScalaClassLoader.asContext$(ScalaClassLoader.scala:116)
  at scala.reflect.internal.util.AbstractFileClassLoader.asContext(AbstractFileClassLoader.scala:43)
  at scala.tools.nsc.interpreter.IMain.loadAndRunReq$1(IMain.scala:505)
  at scala.tools.nsc.interpreter.IMain.$anonfun$doInterpret$3(IMain.scala:519)
  at scala.tools.nsc.interpreter.IMain.doInterpret(IMain.scala:519)
  at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:503)
  at scala.tools.nsc.interpreter.IMain.interpret(IMain.scala:501)
  at scala.tools.nsc.interpreter.shell.ILoop.loop$1(ILoop.scala:878)
  at scala.tools.nsc.interpreter.shell.ILoop.interpretStartingWith(ILoop.scala:906)
  at scala.tools.nsc.interpreter.shell.ILoop.command(ILoop.scala:433)
  at scala.tools.nsc.interpreter.shell.ILoop.processLine(ILoop.scala:440)
  at scala.tools.nsc.interpreter.shell.ILoop.loop(ILoop.scala:458)
  at scala.tools.nsc.interpreter.shell.ILoop.run(ILoop.scala:968)
  at xsbt.ConsoleBridge.run(ConsoleBridge.scala:78)
  at sbt.internal.inc.AnalyzingCompiler.console(AnalyzingCompiler.scala:208)
  at sbt.Console.console0$1(Console.scala:64)
  at sbt.Console.$anonfun$apply$5(Console.scala:74)
  at sbt.Run$.executeSuccess(Run.scala:186)
  at sbt.Console.$anonfun$apply$4(Console.scala:74)
  at sbt.internal.util.Terminal.withRawInput(Terminal.scala:145)
  at sbt.internal.util.Terminal.withRawInput$(Terminal.scala:143)
  at sbt.internal.util.Terminal$ProxyTerminal$.withRawInput(Terminal.scala:384)
  at sbt.Console.$anonfun$apply$3(Console.scala:74)
  at sbt.internal.util.Terminal$TerminalImpl.withRawOutput(Terminal.scala:975)
  at sbt.internal.util.Terminal$ProxyTerminal$.withRawOutput(Terminal.scala:423)
  at sbt.Console.apply(Console.scala:71)
  at sbt.Console.apply(Console.scala:49)
  at sbt.Console.apply(Console.scala:41)
  at sbt.Defaults$.$anonfun$consoleTask$1(Defaults.scala:2232)
  at sbt.Defaults$.$anonfun$consoleTask$1$adapted(Defaults.scala:2218)
  at scala.Function1.$anonfun$compose$1(Function1.scala:49)
  at sbt.internal.util.$tilde$greater.$anonfun$$u2219$1(TypeFunctions.scala:62)
  at sbt.std.Transform$$anon$4.work(Transform.scala:68)
  at sbt.Execute.$anonfun$submit$2(Execute.scala:282)
  at sbt.internal.util.ErrorHandling$.wideConvert(ErrorHandling.scala:23)
  at sbt.Execute.work(Execute.scala:291)
  at sbt.Execute.$anonfun$submit$1(Execute.scala:282)
  at sbt.ConcurrentRestrictions$$anon$4.$anonfun$submitValid$1(ConcurrentRestrictions.scala:265)
  at sbt.CompletionService$$anon$2.call(CompletionService.scala:64)
  at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
  at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
  at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
  at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
  at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
  at java.base/java.lang.Thread.run(Thread.java:829)
```

## TODO
- https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/expressions/UserDefinedAggregateFunction.html
- [UserDefinedAggregateFunction](https://spark.apache.org/docs/2.3.1/sql-programming-guide.html)