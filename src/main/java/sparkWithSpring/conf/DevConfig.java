package sparkWithSpring.conf;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Created by Evegeny on 05/04/2017.
 */
@Configuration
@Profile("DEV")
public class DevConfig {

    @Bean
    public SparkConf sparkConf(){
        return new SparkConf().setAppName("myApp").setMaster("local[*]");
    }



}
