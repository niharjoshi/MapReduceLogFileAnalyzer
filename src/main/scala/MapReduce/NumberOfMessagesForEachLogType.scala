package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.Logger

import scala.collection.JavaConverters.*
import java.lang

object NumberOfMessagesForEachLogType:

  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()
    val logger: Logger = CreateLogger(classOf[TokenizerMapper])

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val logMessage: Array[String] = value.toString.split(" ")
      logger.info("Log message to be processed: " + logMessage.toList.toString)

      val logType = logMessage(2)
      logger.info("Type of log message: " + logType.toString)

      val key = logType.toString
      logger.info("Key created by mapper: " + key.toString)
      word.set(key)

      context.write(word, one)
    }
  }

  class IntSumReader extends Reducer[Text, IntWritable, Text, IntWritable] {

    val logger: Logger = CreateLogger(classOf[IntSumReader])

    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val sum = values.asScala.foldLeft(0)(_ + _.get)
      logger.info("Sum calculated by reducer for key " + key.toString + ": " + sum.toString)

      context.write(key, new IntWritable(sum))
    }
  }

  def run(args: Array[String]): Unit = {

    val TASK_NAME = config.getString("NumberOfMessagesForEachLogType.TaskName")

    val configuration = new Configuration
    val job = Job.getInstance(configuration, TASK_NAME)

    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReader])
    job.setReducerClass(classOf[IntSumReader])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1) + TASK_NAME))

    configuration.set("mapred.textoutputformat.separatorText", ",")

    job.waitForCompletion(true)

  }
