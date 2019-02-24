import org.apache.spark.sql.SparkSession;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.JavaSparkContext;



import org.apache.spark.api.java.function.Function2;


public final class WithTaxation {

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }
  //  $SPARK_HOME/bin/spark-submit --class WithTaxation --master spark://salem:30221 --deploy-mode client demo2.2-1.0-SNAPSHOT.jar links.txt output2 titles.txt
    public static void main(String[] args) throws Exception {


        SparkConf conf = new SparkConf().setAppName("WithTaxation").setMaster("spark://salem:30221");
        JavaSparkContext mySC = new JavaSparkContext(conf);

        JavaRDD<String> myFile = mySC.textFile(args[0]);

        JavaPairRDD<String, String> mylinks = myFile.mapToPair(s -> {
            String[] parts = s.split(":");
            return new Tuple2<>(parts[0], parts[1].trim());
        }).distinct().cache();

        JavaPairRDD<String, Double> myRanks = mylinks.mapValues(rs -> 1.0);

        // Page rank algorithm
        for (int current = 0; current < 25; current++) {
            JavaPairRDD<String, Double> testing = mylinks.join(myRanks).values()
                    .flatMapToPair(s -> {
                        String[] parsedLinks = s._1().split("\\s");
                        int linkCount = parsedLinks.length;
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : parsedLinks) {
                            results.add(new Tuple2<>(n, s._2() / linkCount));
                        }
                        return results.iterator();
                    });


            // page rank taxation
            myRanks = testing.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }
        
        JavaRDD<String> titleFile = mySC.textFile(args[2]);

        AtomicInteger mylineCount = new AtomicInteger(0);
        
        JavaPairRDD<String, String> myTitles = titleFile.mapToPair(s -> {
            mylineCount.addAndGet(1);
            return new Tuple2<>(mylineCount.toString(),s);
        });
        
        JavaRDD<Tuple2<String, Double>> myPageRT = myTitles.join(myRanks).values();
        
        JavaPairRDD<Double, String> swap = myPageRT.mapToPair(s ->
                new Tuple2<>(s._2(), s._1())).sortByKey(false);
        
        JavaPairRDD<String, Double> myPageRank = swap.mapToPair(s -> new Tuple2<>(s._2(), s._1()));

        myPageRank.saveAsTextFile(args[1]);

        mySC.stop();
    }
}