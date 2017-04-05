package sparkWithSpring.words;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import sparkWithSpring.infra.AutowiredBroadcast;

/**
 * Created by Evegeny on 05/04/2017.
 */
@Service
public class PopularWordsCounterImpl implements PopularWordsCounter {
    @AutowiredBroadcast
    private Broadcast<GarbageBag> garbageBag;


    @Override
    public Tuple2<Integer, String> topX(JavaRDD<String> lines) {
        JavaRDD<String> words = lines.map(String::toLowerCase).flatMap(WordsUtil::getWords);
        words = words.filter(word -> !this.garbageBag.value().garbage.contains(word));
        return words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false).first();
    }
}










