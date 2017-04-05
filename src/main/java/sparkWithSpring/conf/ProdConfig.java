package sparkWithSpring.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.TorrentBroadcast;
import org.apache.spark.storage.BroadcastBlockId;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by Evegeny on 05/04/2017.
 */
@Configuration
@Profile("PROD")
public class ProdConfig {

    @Bean
    public SparkConf sparkConf(){
       /* SparkConf conf = new SparkConf();
        conf.setAppName("mbi etl");
        conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired","false");

        Reflections com = new Reflections("com....","com...");
        Set<Class<? extends Serializable>> classes = com.getSubTypesOf(Serializable.class);
        Class[] starhomeSerializableClasses = classes.toArray(new Class[classes.size()]);
        conf.registerKryoClasses(new Class[]{Object[].class, HashMap.class, ArrayList.class,LinkedHashSet.class,TorrentBroadcast.class,BroadcastBlockId.class});
        conf.registerKryoClasses(starhomeSerializableClasses);
        return conf;*/
        return null;
    }



}
