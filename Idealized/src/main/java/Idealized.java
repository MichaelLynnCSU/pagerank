import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.Function2;


public final class Idealized {


    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static AtomicInteger mylineCount = new AtomicInteger();

    private static class helperF {
        public Tuple2<String, String> increment (String inc) {
            String count = Integer.toString(mylineCount.incrementAndGet());
            Tuple2<String, String> mycount = new Tuple2<>(count, inc);
            return mycount;
        }
    }

    private static final Pattern COLON = Pattern.compile(":");

    public static void main(String[] argv){
        if (argv.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <titles>\n",
                    Idealized.class.getSimpleName());
            return;
        }

        String inputPath = argv[0];
        String outputPath = argv[1];
        String titles = argv[2];

        SparkConf conf = new SparkConf().setAppName("Idealized").setMaster("spark://salem:30221");
        JavaSparkContext mySC = new JavaSparkContext(conf);

        JavaRDD<String> myFile = mySC.textFile(inputPath);

        // Store urls and there associated neighbors.
        JavaPairRDD<String, String> mylinks = myFile.mapToPair(s -> {
            String[] myparts = COLON.split(s);
            return new Tuple2<>(myparts[0], myparts[1].trim());
        }).distinct().cache();


        JavaPairRDD<String, Double> myRanks = mylinks.mapValues(rs -> 1.0);

        // Page rank algorithm
        for (int current = 0; current < 25; current++) {
            JavaPairRDD<String, Double> testing = mylinks.join(myRanks).values()
                    .flatMapToPair(s -> {
                        String[] mysplits = s._1.split("\\s+");
                        int urlCount = mysplits.length;

                        List<Tuple2<String, Double>> myresults = new ArrayList<>();
                        for (String n: mysplits) {
                            myresults.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return myresults.iterator();
                    });
            myRanks = testing.reduceByKey(new Sum()).mapValues(sum -> sum);
        }


        JavaRDD<String> allTitles = mySC.textFile(titles);

        // rank and title return
        JavaPairRDD<String, String> lineTitles = allTitles.mapToPair(s -> {
            helperF sum = new helperF();
            return sum.increment(s);
        }).distinct().cache();

        // join line titles
        JavaPairRDD<String, Tuple2<String, Double>> myJoined = lineTitles.join(myRanks);

        JavaPairRDD<Double,String> myOut =
                myJoined.values().mapToPair((t)->new Tuple2(
                        t._2,
                        t._1));

        // sorting the list
        myOut.sortByKey(false).saveAsTextFile(outputPath);
    }
}
