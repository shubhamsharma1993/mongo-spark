package org.test.mongodb_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import com.mongodb.spark.*;
import org.bson.Document;
import java.util.*;
import java.util.HashMap;
import static java.util.Arrays.asList;
import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;


public class App 
{
    public static void main( String[] args )
    {
        
    	SparkConf sc = new SparkConf()
    	.setMaster("local")
    	.setAppName("MongoSparkConnection")
    	.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
    	.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection");
    	
    	JavaSparkContext jsc = new JavaSparkContext(sc); // create a java spark context
    	
    	Map<String, String> writeOverrides = new HashMap<String, String>();
    	writeOverrides.put("collection", "spark");
    	writeOverrides.put("writeConcern.w", "majority");
    	WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

    	JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
    	    (new Function<Integer, Document>() {
    	        public Document call(final Integer i) throws Exception {
    	            return Document.parse("{spark: " + i + "}");
    	        }
    	    });

    	// Saving data from an RDD to MongoDB
    	MongoSpark.save(sparkDocuments, writeConfig);
    	
    	
        
        
        
        
    }
}
