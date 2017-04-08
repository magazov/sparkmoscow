package sparkWithSpring.words;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import scala.Tuple2;
import sparkWithSpring.conf.AppConfig;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by Evegeny on 05/04/2017.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AppConfig.class)
@ActiveProfiles("DEV")
public class PopularWordsCounterImplTest {

    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private PopularWordsCounter counter;


    @Test
    public void fromRealFile() throws Exception {
        JavaRDD<String> rdd = sc.textFile("data/songs/califronia.txt");
        Tuple2<Integer, String> topX = counter.topX(rdd);
        System.out.println("topX = " + topX);
    }

    @Test
    public void topX() throws Exception {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("java spring", "java groovy", "spring spring", "spring java spring"));
        Tuple2<Integer, String> tuple2 = counter.topX(rdd);
        System.out.println("tuple2 = " + tuple2);

    }

}