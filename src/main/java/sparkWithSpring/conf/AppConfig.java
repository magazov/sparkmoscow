package sparkWithSpring.conf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Created by Evegeny on 05/04/2017.
 */
@Configuration
@ComponentScan(basePackages = "sparkWithSpring")
@PropertySource("classpath:user.properties")
public class AppConfig {
    @Autowired
    private SparkConf sparkConf;

    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sc());
    }

    @Bean
    public JavaSparkContext sc(){
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.setLogLevel("WARN");
        Logger.getRootLogger().setLevel(Level.OFF);
        return javaSparkContext;
    }




}
