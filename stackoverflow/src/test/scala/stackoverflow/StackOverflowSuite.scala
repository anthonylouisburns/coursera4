package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow.{groupedPostings, rawPostings, sc, scoredPostings}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  //  test("testObject can be instantiated") {
  //    val instantiatable = try {
  //      testObject
  //      true
  //    } catch {
  //      case _: Throwable => false
  //    }
  //    assert(instantiatable, "Can't instantiate a StackOverflow object")
  //  }

  //  test("answers with no parentId"){
  //    val lines   = sc.textFile("/Users/aburns/src/anthony/coursera/scalaSpecialization/coursera4/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv")
  //    val raw     = rawPostings(lines)
  //    raw.take(5).map(line => println(line))
  //  }

//  test("Idea for test: 𝚜𝚌𝚘𝚛𝚎𝚍 RDD should have 2121822 entries") {
//    val lines = sc.textFile("/Users/aburns/src/anthony/coursera/scalaSpecialization/coursera4/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv")
//    println("lines " + lines.count())
//    val raw = rawPostings(lines)
//    println("raw " + raw.count())
//    val grouped = groupedPostings(raw)
//    println("grouped " + grouped.count())
//    val scored  = scoredPostings(grouped)
//    val numScored = scored.count()
//    println("scored " + numScored)
//    assert(numScored === 2121822L)
//  }
//
//  test("Idea for test: 𝚜𝚌𝚘𝚛𝚎𝚍 RDD should have 2121822 entries") {
//    val lines1 = sc.textFile("/Users/aburns/src/anthony/coursera/scalaSpecialization/coursera4/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv").take(10000)
//    val lines = sc.parallelize(lines1)
//    println("lines " + lines.count())
//    val postings = rawPostings(lines)
//    println("raw " + postings.count())
//
//    val questions = postings.filter(p=> p.postingType==1)
//    println("questions " + questions.count())
//
//    val answers = postings.filter(p=> p.postingType==2)
//    println("answers " + answers.count())
//    val qid_questions:RDD[(Int,Posting)] = questions.map(p=>(p.id, p))
//    println("qid_questions " + qid_questions.count())
//    val qid_answers:RDD[(Int,Posting)]  = answers.map(p=>(p.parentId.get, p))
//    println("qid_answers " + qid_answers.count())
//    val grouped = qid_questions.join(qid_answers).groupByKey()
//    println("grouped " + grouped.count())
//
//  }
//
//  test("cluster size") {
//    val lines1 = sc.textFile("/Users/aburns/src/anthony/coursera/scalaSpecialization/coursera4/stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv").take(10000)
//    val lines = sc.parallelize(lines1)
//    println("lines " + lines.count())
//    val postings = rawPostings(lines)
//    println("raw " + postings.count())
//
//    val questions = postings.filter(p=> p.postingType==1)
////    println("questions " + questions.filter(p=>p.).count())
//
//    val answers = postings.filter(p=> p.postingType==2)
//    println("answers " + answers.count())
//    val qid_questions:RDD[(Int,Posting)] = questions.map(p=>(p.id, p))
//    println("qid_questions " + qid_questions.count())
//    val qid_answers:RDD[(Int,Posting)]  = answers.map(p=>(p.parentId.get, p))
//    println("qid_answers " + qid_answers.count())
//    val grouped = qid_questions.join(qid_answers).groupByKey()
//    println("grouped " + grouped.count())
//
//  }

  test("clusterResults"){
    val centers = Array((0,0), (100000, 0))
    val rdd = StackOverflow.sc.parallelize(List(
      (0, 1000),
      (0, 23),
      (0, 234),
      (0, 0),
      (0, 1),
      (0, 1),
      (50000, 2),
      (50000, 10),
      (100000, 2),
      (100000, 5),
      (100000, 10),
      (200000, 100),
      (100000, 2444),
      (100000, 5444),
      (100000, 10444),
      (200000, 100444)  ))
    testObject.printResults(testObject.clusterResults(centers, rdd))
  }
}
