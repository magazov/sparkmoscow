package taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

/**
 * Created by Evegeny on 05/04/2017.
 */
public class TaxiMain {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("taxi").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("data/taxi_order.txt");
        JavaRDD<Trip> rddTrips = rdd.map(String::toLowerCase).
                map(FunctionUtils::conertLineToTrip);
        rddTrips.persist(StorageLevel.MEMORY_AND_DISK());
        long count = rddTrips.count();
        System.out.println("count = " + count);

        long bostonTripsCount = rddTrips
                .filter(trip -> trip.getDistance()>10)
                .filter(trip -> trip.getCity().equalsIgnoreCase("boston")).count();
        System.out.println("bostonTripsCount = " + bostonTripsCount);

        Double bostonTripsSum = rddTrips.filter(trip -> trip.getCity().equalsIgnoreCase("boston"))
                .mapToDouble(Trip::getDistance).sum();
        System.out.println("bostonTripsSum = " + bostonTripsSum);
        JavaPairRDD<String, Integer> distanceRdd = rddTrips.mapToPair(trip -> new Tuple2<>(trip.getDriverId(), trip.getDistance()));
        JavaRDD<String> driversRdd = sc.textFile("data/drivers.txt");
        JavaPairRDD<String, String> driversPairRdd = driversRdd.mapToPair(line -> {
            String[] arr = line.split(",");
            return new Tuple2<>(arr[0], arr[1]);
        });


        JavaPairRDD<String, Tuple2<Integer, String>> joinRdd = distanceRdd.reduceByKey(Integer::sum).join(driversPairRdd);
        joinRdd.collect().forEach(System.out::println);

        joinRdd.mapToPair(Tuple2::_2).
                sortByKey((Serializable & Comparator<Integer>)(o1, o2) -> o1.compareTo(o2)).take(3).forEach(System.out::println);



    }
}







