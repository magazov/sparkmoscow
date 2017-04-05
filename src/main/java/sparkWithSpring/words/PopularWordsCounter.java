package sparkWithSpring.words;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by Evegeny on 05/04/2017.
 */
public interface PopularWordsCounter extends Serializable {
    Tuple2<Integer, String> topX(JavaRDD<String> words);
}
