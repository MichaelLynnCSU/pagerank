import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public final class Bomb {


  
  
    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }
	
  public static AtomicInteger myLC = new AtomicInteger();
    private static class Add {
        public Tuple2<String, String> increment (String s) 
		{
            String myCount = Integer.toString(myLC.incrementAndGet());
            Tuple2<String, String> sum = new Tuple2<>(myCount, s);
            return sum;
        }
    }
   private static final Pattern COLON = Pattern.compile(":");

    public static void main(String[] argv){
        if (argv.length != 3) {
            System.err.printf("Usage: [generic options] <input> <output> <titles>\n");
            return;
        }

        String inputPath = argv[0];
        String outputPath = argv[1];
        String titles = argv[2];


        SparkConf conf = new SparkConf().setAppName("Bomb").setMaster("spark://salem:30221");
        JavaSparkContext mySC = new JavaSparkContext(conf);

        JavaRDD<String> file = mySC.textFile(inputPath);

     

        JavaPairRDD<String, String> l = file.mapToPair(s -> {
            String[] mypart = COLON.split(s);
            String myBomb = mypart[1] + " 2";
            return new Tuple2<>(mypart[0], myBomb.trim());
        }).distinct().cache();


        JavaRDD<String> at = mySC.textFile(titles);

        JavaPairRDD<String, String> mytitles = at.mapToPair(s -> {
            Add sum = new Add();
            return sum.increment(s);
        }).distinct().cache();

        JavaPairRDD filter = mytitles.filter(line -> line._2.toLowerCase().contains("surfing"));

        JavaPairRDD b = mytitles.filter(line -> line._2.toLowerCase().contains("rocky_mountain_national_park"));

        JavaPairRDD <String, Tuple2<String, String>> filteredLinks = filter.join(l);
        JavaPairRDD <String, Tuple2<String, String>> bl = b.join(l);

        JavaPairRDD <String, Tuple2<String,String>> fl = bl.union(filteredLinks);
        JavaPairRDD<String, String> finalLinks = fl.mapToPair(s -> new Tuple2<>(s._1(), s._2()._2()));
        JavaPairRDD<String, Double> myRank = l.mapValues(rs -> 1.0);


        for (int current = 0; current < 25; current++) {
            JavaPairRDD<String, Double> k = l.join(myRank).values().flatMapToPair(s -> {
                        String[] splitVals = s._1.split("\\s+");
                        int urlCount = splitVals.length;

                        List<Tuple2<String, Double>> myResults = new ArrayList<>();
						
                        for (String n: splitVals) {
							
                            myResults.add(new Tuple2<>(n, s._2() / urlCount));
							
                        }
                        return myResults.iterator();
						
                    });
					
            myRank = k.reduceByKey(new Sum()).mapValues(sum -> sum);
        }

        JavaPairRDD<String, Tuple2<String, Double>> myJoin = mytitles.join(myRank);

        JavaPairRDD<Double,String> res =
                myJoin.values().mapToPair((t)->new Tuple2(
                        t._2,
                        t._1));

        res.sortByKey(false).saveAsTextFile(outputPath);

    }
}
