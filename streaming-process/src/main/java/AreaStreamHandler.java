import bean.AreaStream;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class AreaStreamHandler {

    private static void handleStream(JavaSparkContext sparkContext) {
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));
        JavaDStream<String> contentList = streamingContext.textFileStream("streamInput/area/");
        //计算每个省份的每月票房
        JavaPairDStream<String, AreaStream> boxPerAreaPerMonth = contentList
                .flatMap(content -> {
                    JSONArray jsonArray = JSONArray.parseArray(content);
                    List<AreaStream> res = new ArrayList<>();
                    for (int i = 0; i < jsonArray.size(); i++) {
                        res.add(JSONObject.parseObject(jsonArray.getString(i), AreaStream.class));
                    }
                    return res.iterator();
                }).mapToPair(
                        areaStream -> {
                            areaStream.setTime(areaStream.getTime().substring(0, 7));
                            return new Tuple2<>(areaStream.getProvince(), areaStream);
                        }
                ).reduceByKey((a, b) -> {
                    AreaStream areaStream = new AreaStream();
                    areaStream.setProvince(a.getProvince());
                    areaStream.setProvinceId(a.getProvinceId());
                    areaStream.setTime(a.getTime());
                    areaStream.setBox(a.getBox() + b.getBox());
                    return areaStream;
                });

        boxPerAreaPerMonth.print();

        boxPerAreaPerMonth.foreachRDD(rdd -> {
            JavaRDD<Document> documents = rdd.map(t -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("province", t._2().getProvince());
                jsonObject.put("time", t._2().getTime());
                jsonObject.put("province_id", t._2().getProvinceId());
                jsonObject.put("total_month_box", t._2().getBox());
                return  Document.parse(jsonObject.toJSONString());
            });
            MongoSpark.save(documents);
        });

        streamingContext.start();              // Start the computation
        try {
            streamingContext.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder()
                .appName("StreamingProcess")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/")
                .config("spark.mongodb.output.database", "sparkpractise")
                .config("spark.mongodb.output.collection", "area_box")
                .getOrCreate();
//        SparkConf sparkConf = new SparkConf().setAppName("myStreaming");
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        handleStream(sparkContext);
    }

}
