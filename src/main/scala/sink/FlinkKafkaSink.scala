package sink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, TextInputFormat, TextOutputFormat}

object FlinkKafkaSink {
  def getHiveSink(args: Array[String]): Unit = {

    //val sinkEnv = StreamExecutionEnvironment.getExecutionEnvironment

   /* val benv = ExecutionEnvironment.getExecutionEnvironment
    val input = benv.readHadoopFile(new TextInputFormat, classOf[LongWritable], classOf[Text], textPath)

    val text = input map { _._2.toString }
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    val words = counts map { t => (new Text(t._1), new LongWritable(t._2)) }

    val hadoopOutputFormat = new HadoopOutputFormat[Text,LongWritable](
      new TextOutputFormat[Text, LongWritable], new JobConf)
    val c = classOf[org.apache.hadoop.io.compress.GzipCodec]
    hadoopOutputFormat.getJobConf.set("mapred.textoutputformat.separator", " ")
    hadoopOutputFormat.getJobConf.setCompressMapOutput(true)
    hadoopOutputFormat.getJobConf.set("mapred.output.compress", "true")
    hadoopOutputFormat.getJobConf.setMapOutputCompressorClass(c)
    hadoopOutputFormat.getJobConf.set("mapred.output.compression.codec", c.getCanonicalName)
    hadoopOutputFormat.getJobConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf, new Path("/tmp/iteblog/"))

    words.output(hadoopOutputFormat)
    benv.execute("Hadoop Compat WordCount")*/


  }
}
