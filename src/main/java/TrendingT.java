import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static java.lang.System.exit;
import static javafx.scene.input.KeyCode.T;

/**
 * Created by pilarpineiro on 20/4/16.
 */
public class TrendingT {

    public static void main(String[] args) {
        if (args.length==0){
            throw new RuntimeException ("No arguments");

        }

        SparkConf conf = new SparkConf().setAppName("TrendingT");

        JavaSparkContext sparkContext= new JavaSparkContext(conf);

        JavaRDD<String> lines= sparkContext.textFile(args[0]);

        //divide lines and keep the column with the tweets
        JavaRDD<String> tweets= lines.map(new Function<String,String>(){
            public String call(String s){
                String[] split= s.toString().split("\t+");

                return split[2];

            }
        });




        //now i have the tweets and i need to split the lines into words:

        JavaRDD<String> words= tweets.flatMap(new FlatMapFunction<String,String>(){

            public Iterable call(String s) throws Exception{
                return Arrays.asList(s.split(" "));


            }

        });




        //making pairs of word,1
        JavaPairRDD<String,Integer> singleword=words.mapToPair(
                new PairFunction<String,String,Integer>(){
                    public Tuple2<String,Integer> call(String string){
                        return new Tuple2<String,Integer>(string,1);
                    }
                });




        //total sum of all times a word appears:
        JavaPairRDD<String,Integer> total= singleword.reduceByKey(
                new Function2<Integer,Integer,Integer>(){
                    public Integer call(Integer integer, Integer integer2) throws Exception{
                        return integer+integer2;
                    }
                }

        );

        // if i want to order the words by number of repetitions, i have to invert the map:
        JavaPairRDD<Integer,String> invert=total.mapToPair(
            new PairFunction<Tuple2<String,Integer>,Integer,String>(){
                public Tuple2<Integer,String> call(Tuple2<String,Integer> stringIntegerTuple2) throws Exception{
                    return new Tuple2<Integer,String>(stringIntegerTuple2._2,stringIntegerTuple2._1);
                }
            }
        );

        //now, we will order by key the inverted map

        List<Tuple2<Integer,String>> sortedWords= invert.sortByKey().collect();

        for (Tuple2<?,?> tuple:sortedWords){
            System.out.println(tuple._1()+":" + tuple._2());

        }

     invert.sortByKey().saveAsTextFile(args[1]);

        sparkContext.stop();

    }



    }
