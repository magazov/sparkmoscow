package sparkWithSpring.conf;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.apache.spark.sql.functions.*;
import static org.junit.Assert.*;

/**
 * Created by Evegeny on 05/04/2017.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = AppConfig.class)
@ActiveProfiles("DEV")
public class DataFrameWorkTest {
    @Autowired
    private SQLContext sqlContext;

    @Test
    public void linkedIn() throws Exception {
        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*.json");
        dataFrame.show();
        dataFrame.printSchema();
        StructField[] structFields = dataFrame.schema().fields();
      /*  for (StructField structField : structFields) {
            structField.dataType()
        }*/

        dataFrame = dataFrame.withColumn("salary",
                col("age").multiply(10).multiply(size(col("keywords"))));

        DataFrame keyWordsDf = dataFrame.withColumn("keyword", explode(col("keywords"))).select("keyword");
        keyWordsDf = keyWordsDf.groupBy("keyword").agg(count("keyword").as("amount"));
        keyWordsDf.show();
        Row row = keyWordsDf.orderBy(col("amount").desc()).head();
        String mostPopular = row.getAs("keyword");
        System.out.println("mostPopular = " + mostPopular);

        dataFrame.filter(col("salary").leq(1400).
                and(array_contains(col("keywords"), mostPopular))).show();



    }

}















