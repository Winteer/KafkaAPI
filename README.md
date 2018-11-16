# KafkaAPI
KafkaAPI（include Producer，Consumer Demo, and record offset to localpath）

/**
  *ConsumerDemoByOffset.java  提供了记录offset到本地的功能来消费数据。
  *原因：若消费者运行机器出故障，或程序异常结束，可从本地记录文件中找到已接收到的offset进行继续接收。避免从头再来。
  **/
  
offset本地记录文件为txt文件。内容如下：

KafkaOffsetLog:
0

