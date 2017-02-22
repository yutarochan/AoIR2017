/*
 * AoIR 2017 - Presidential Candidate Debate Twitter Analytics
 * Author: Yuya Jeremy Ong (yuyajeremyong@gmail.com)
 */
package demo

import java.io.File
import java.util.Arrays
import java.io.PrintWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;


object Demo {
    // Application Specific Variables
 	private final val SPARK_MASTER = "yarn-client"
 	private final val APPLICATION_NAME = "presidential-debate"

 	// HDFS Configuration Files
 	private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
 	private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")

    def main(args: Array[String]): Unit = {
        // Configure SparkContext
 		val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
 		val sc = new SparkContext(conf)

        // Configure HDFS
		val configuration = new Configuration();
		configuration.addResource(CORE_SITE_CONFIG_PATH);
		configuration.addResource(HDFS_SITE_CONFIG_PATH);
    }
}
