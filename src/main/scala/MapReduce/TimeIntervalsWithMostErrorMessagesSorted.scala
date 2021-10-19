package MapReduce

import HelperUtils.{CreateLogger, ObtainConfigReference}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.Logger

import scala.collection.JavaConverters.*
import java.lang
import java.util.regex.Pattern

object TimeIntervalsWithMostErrorMessagesSorted:

  val config = ObtainConfigReference("randomLogGenerator") match {
    case Some(value) => value
    case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
  }

  class TokenizerMapper extends Mapper[Object, Text, Text, Text] {

    val word = new Text()
    val logger: Logger = CreateLogger(classOf[TokenizerMapper])

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {

      val logMessage: Array[String] = value.toString.split("\t")
      logger.info(s"Log message to be processed: ${logMessage.toList}")

      val key = s"${logMessage(1)}"
      logger.info(s"Key created by mapper: ${key}")
      word.set(key)

      val data = s"${logMessage{0}}"
      logger.info(s"Data created by mapper: ${key}")

      context.write(word, new Text(data))
    }
  }

  def run(args: Array[String]): Unit = {

    val TASK_NAME = config.getString("TimeIntervalsWithMostErrorMessagesSorted.TaskName")

    val configuration = new Configuration
    val job = Job.getInstance(configuration, TASK_NAME)

    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    val inputFile = config.getString("TimeIntervalsWithMostErrorMessagesSorted.InputFile")
    FileInputFormat.addInputPath(job, new Path(inputFile))
    FileOutputFormat.setOutputPath(job, new Path(args(1) + TASK_NAME))

    configuration.set("mapred.textoutputformat.separatorText", ",")

    job.waitForCompletion(true)

  }
