package org.test.mongodb_spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.mongodb.spark.*;

import org.bson.Document;

import java.util.*;

import static java.util.Arrays.asList;

import com.mongodb.spark.config.WriteConfig;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;

import com.mongodb.spark.config.ReadConfig;

public class App 
{
    public static void main( String[] args )
    {
        
    	SparkConf sc = new SparkConf()
    	.setMaster("local")
    	.setAppName("MongoSparkConnection")
    	.set("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myNewColl")
    	.set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myNewColl");
    	
    	JavaSparkContext jsc = new JavaSparkContext(sc); // create a java spark context
    	
    	Map<String, String> writeOverrides = new HashMap<String, String>();
    	writeOverrides.put("collection", "myNewColl");
    	WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

    	List<String> characters = asList(
    	    "{'name': 'Bilbo Baggins', 'age': 50}",
    	    "{'name': 'Gandalf', 'age': 1000}",
    	    "{'name': 'Thorin', 'age': 195}",
    	    "{'name': 'Balin', 'age': 178}",
    	    "{'name': 'Kíli', 'age': 77}",
    	    "{'name': 'Dwalin', 'age': 169}",
    	    "{'name': 'Óin', 'age': 167}",
    	    "{'name': 'Glóin', 'age': 158}",
    	    "{'name': 'Fíli', 'age': 82}",
    	    "{'name': 'Bombur'}"
    	);
    	MongoSpark.save(jsc.parallelize(characters).map(new Function<String, Document>() {
    	    public Document call(final String json) throws Exception {
    	        return Document.parse(json);
    	    }
    	}), writeConfig);
    	
    	

    	// Loading data with a custom ReadConfig
    	Map<String, String> readOverrides = new HashMap<String, String>();
    	readOverrides.put("collection", "myNewColl");
    	readOverrides.put("readPreference.name", "secondaryPreferred");
    	ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

    	JavaRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);
    	
    	
        System.out.println(customRdd.count());
        System.out.println(customRdd.first().toJson());
    	
      
    	SQLContext sqlContext = SQLContext.getOrCreate(jsc.sc());
    
    	DataFrame df = MongoSpark.load(jsc).toDF();
    	df.printSchema();
    	df.registerTempTable("characters");
    			
    	 DataFrame centenarians = sqlContext.sql("SELECT name, age FROM characters WHERE age <= 100");
    	MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

    	// Load the data from the "hundredClub" collection
    	MongoSpark.load(sqlContext, ReadConfig.create(sqlContext).withOption("collection", "hundredClub"), Character.class).show();
    	
    	
    
    
    
    }
}
