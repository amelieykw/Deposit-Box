// spamFilter.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD



  def probaWordDir(sc:SparkContext)(filesDir:String)
  :(RDD[(String, Double)], Long) = {
  val files = sc.wholeTextFiles(filesDir + "/*.txt").collect.toList
  val nbFiles = files.length
  val wordset = files.map(x => x._2.split("\\W+").distinct.toSet)
  // res25: List[scala.collection.immutable.Setobject spamFilter {
  // [String]] = List(Set(green, orange, yellow, red), Set(banana, orange, green, apple, kiwi, pear, red), Set(salad, potato, carrot, green, leek))
  val wordDirOccurency = sc.parallelize(wordset.flatMap(x => x.map(word => (word,1)))).reduceByKey((x,y) => (x+y))
  // res26: Array[(String, Int)] = Array((kiwi,1), (yellow,1), (banana,1), (salad,1), (red,2), (pear,1), (orange,2), (apple,1), (green,3), (carrot,1), (leek,1), (potato,1))
  val probaWord = wordDirOccurency.map(x => (x._1, x._2.toDouble/nbFiles))
  // res31: Array[(String, Double)] = Array((kiwi,0.3333333333333333), (yellow,0.3333333333333333), (banana,0.3333333333333333), (salad,0.3333333333333333), (red,0.6666666666666666), (pear,0.3333333333333333), (orange,0.6666666666666666), (apple,0.3333333333333333), (green,1.0), (carrot,0.3333333333333333), (leek,0.3333333333333333), (potato,0.3333333333333333))
  return (probaWord, nbFiles)
  }

  def probaWordDir2Dirs(sc:SparkContext)(filesDir1:String, filesDir2:String)
  :(RDD[(String, Double)], Long) = {

  val files = sc.wholeTextFiles(filesDir1 + "/*.txt").collect.toList ::: sc.wholeTextFiles(filesDir2 + "/*.txt").collect.toList
  val nbFiles = files.length
  val wordset = files.map(x => x._2.split("\\W+").distinct.toSet)
  // res25: List[scala.collection.immutable.Set[String]] = List(Set(green, orange, yellow, red), Set(banana, orange, green, apple, kiwi, pear, red), Set(salad, potato, carrot, green, leek))
  val wordDirOccurency = sc.parallelize(wordset.flatMap(x => x.map(word => (word,1)))).reduceByKey((x,y) => (x+y))
  // res26: Array[(String, Int)] = Array((kiwi,1), (yellow,1), (banana,1), (salad,1), (red,2), (pear,1), (orange,2), (apple,1), (green,3), (carrot,1), (leek,1), (potato,1))
  val probaWord = wordDirOccurency.map(x => (x._1, x._2.toDouble/nbFiles))
  // res31: Array[(String, Double)] = Array((kiwi,0.3333333333333333), (yellow,0.3333333333333333), (banana,0.3333333333333333), (salad,0.3333333333333333), (red,0.6666666666666666), (pear,0.3333333333333333), (orange,0.6666666666666666), (apple,0.3333333333333333), (green,1.0), (carrot,0.3333333333333333), (leek,0.3333333333333333), (potato,0.3333333333333333))
  return (probaWord, nbFiles)
  }

  def computeMutualInformationFactor(
    probaWC:RDD[(String, Double)],  // word => probability the word occurs (or not) in an email of a given class
    probaW:RDD[(String, Double)],  // word => probability the word occurs (whatever the class)
    probaC: Double,  // the probability that an email belongs to the given class
    probaDefault: Double // default value when a probability is missing 
    // When a word does not occur in both classes but only one, its probability 𝑃(𝑜𝑐𝑐𝑢𝑟𝑠, 𝑐𝑙𝑎𝑠𝑠) must take on the default value probaDefault. 
  ):RDD[(String, Double)] = {

     probaW.leftOuterJoin(probaWC).mapValues { 
        case (x, Some(y)) => y * math.log(y/(x*probaC))
        case (x, None) => probaDefault * math.log(probaDefault/(x * probaC))
    }
  }

  def main(args: Array[String]) {
    val (probaWordHam, nbFilesHam) : RDD[(String, Double)], Long) = probaWordDir(sc)("/tmp/ling-spam/ham")
    val (probaWordSpam, nbFilesSpam) : RDD[(String, Double)], Long) = probaWordDir(sc)("/tmp/ling-spam/spam")
    val (probaWord, nbFiles) : RDD[(String, Double)], Long) = probaWordDir(sc)("/tmp/ling-spam/ham", "/tmp/ling-spam/Spam")
    // formule
    val probaAbsentWordSpam = probaWordSpam.mapValues { 1.0 - _ }
    val probaAbsentWordHam = probaWordHam.mapValues { 1.0 - _ }
    // 𝑃(𝑐𝑙𝑎𝑠𝑠)
    val probaSpam =  nbFilesSpam.toDouble / nbFiles
    val probaHam = 1.0 - probaSpam
    // les 4 𝑃(𝑜𝑐𝑐𝑢𝑟𝑠, 𝑐𝑙𝑎𝑠𝑠)
    val probaPresentAndSpam = probaWordSpam.mapValues {_ * probaSpam}
    val probaPresentAndHam = probaWordHam.mapValues { _ * probaHam }
    val probaAbsentAndSpam = probaAbsentWordSpam.mapValues { _ * probaSpam }
    val probaAbsentAndHam = probaAbsentWordHam.mapValues { _ * probaHam }
    // 𝑃(𝑜𝑐𝑐𝑢𝑟𝑠)
    val probaWordAbsent = probaWord.mapValues { 1.0 - _ }
    // probaDefault
    val probaDefault = 0.2/nbFiles
    // word => MI(word)
    val MI = List(
      computeMutualInformationFactor(probaPresentAndSpam, probaWord, probaSpam, probaDefault),
      computeMutualInformationFactor(probaPresentAndHam, probaWord, probaHam, probaDefault),
      computeMutualInformationFactor(probaAbsentAndSpam, probaWordAbsent, probaSpam, probaDefault),
      computeMutualInformationFactor(probaAbsentAndHam, probaWordAbsent, probaHam, probaDefault)
    ).reduce { 
      (term1, term2) => term1.join(term2).mapValues { 
         case (l, r) => l + r 
      } 
    }
    // the 10 top words (maximizing the mutual information value)
    val resultat = MI.takeOrdered(10)(Ordering.by { _ => _._2 })
        .foreach { println }
    }
    resultat.saveAsTextFile("hdfs://tmp/ling-Spam/resultat.txt")
  }

} // end of spamFilter 





