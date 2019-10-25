import bean.FilmBox;
import bean.FilmStream;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by shea on 2019/10/23.
 */
public class FilmStreamHandler {

    private static void computeActorPerYear(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, Broadcast<String> month, JavaSparkContext sparkContext) {
        WriteConfig writeConfig = WriteConfig.create(sparkContext);
        writeConfig.withOption("collection", "boxPerActorPerYear");
        JavaPairDStream<Tuple2<String, String>, Tuple2<Long, Long>> boxPerActorPerYear = boxPerFilmPerMonth
                .flatMapToPair(
                        element -> {
                            List<Tuple2<Tuple2<String, String>, Tuple2<Long, Long>>> res = new ArrayList<>();
                            long onlineBox = element._2().getOnlineBox();
                            long totalBox = element._2().getTotalBox();
                            String year = element._2().getTime().substring(0, 4);
                            for (int i = 0; i < element._2().getActors().size(); i++) {
                                res.add(new Tuple2<>(new Tuple2<>(element._2().getActors().get(i), year), new Tuple2<>(onlineBox, totalBox)));
                            }
                            return res.iterator();
                        }
                ).reduceByKeyAndWindow(
                        (a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()),
                        Durations.seconds(12), Durations.seconds(12)
                );
        boxPerActorPerYear.foreachRDD(pairRdd -> {
            JavaRDD<Document> documents = pairRdd.map(t -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("actor", t._1()._1());
                jsonObject.put("time", t._1()._2());
                jsonObject.put("month", month.value());
                jsonObject.put("total_year_box", t._2()._2());
                jsonObject.put("online_year_box", t._2()._1());
                Document document = Document.parse(jsonObject.toJSONString());
                return document;
            });
            MongoSpark.save(documents, writeConfig);
        });

    }


    private static void computeLocationRatePerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sc, Broadcast<String> month, JavaSparkContext sparkContext) {
        WriteConfig writeConfig = WriteConfig.create(sparkContext);
        writeConfig.withOption("collection", "locationRatePerMonth");
        LongAccumulator onlineBoxAccum = sc.sc().longAccumulator();
        LongAccumulator totalBoxAccum = sc.sc().longAccumulator();
        JavaPairDStream<String, Tuple3<Long, Long, String>> perLocationPerMonth = boxPerFilmPerMonth
                .mapToPair(
                        element -> {
                            onlineBoxAccum.add(element._2().getOnlineBox());
                            totalBoxAccum.add(element._2().getTotalBox());
                            return new Tuple2<>(element._2().getLocation(), new Tuple3<>(element._2().getOnlineBox(), element._2().getTotalBox(), element._2().getTime()));
                        }
                );
        Double onlineBox = Double.valueOf(onlineBoxAccum.sum());
        Double totalBox = Double.valueOf(totalBoxAccum.sum());
        Broadcast<Tuple2<Double, Double>> boxes = sc.broadcast(new Tuple2<>(onlineBox, totalBox));
        perLocationPerMonth
                .reduceByKey((a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(), a._3()))
                .mapValues(x -> new Tuple3<>(x._1() / boxes.value()._1(), x._2() / boxes.value()._2(), x._3()))
                .foreachRDD(
                        pairRdd -> {
                            JavaRDD<Document> documents = pairRdd.map(t -> {
                                JSONObject jsonObject = new JSONObject();
                                jsonObject.put("location", t._1());
                                jsonObject.put("time", t._2()._3());
                                jsonObject.put("box_percent", t._2()._2());
                                Document document = Document.parse(jsonObject.toJSONString());
                                return document;
                            });
                            MongoSpark.save(documents, writeConfig);
                        }
                );
        writeConfig.withOption("collection", "boxPerMonth");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("time", month.value());
        jsonObject.put("total_month_box", totalBox);
        jsonObject.put("online_month_box", onlineBox);
        Document document = Document.parse(jsonObject.toJSONString());
        JavaRDD<Document> boxPerMonth = sc.parallelize(Arrays.asList(document));
        MongoSpark.save(boxPerMonth, writeConfig);

    }

    private static void computeBoxPerTypePerMonth(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "boxPerTypePerMonth");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
//        writeConfig.withOption("collection", "boxPerTypePerMonth");
        JavaPairDStream<String, Tuple3<Long, Long, String>> boxPerTypePerMonth = boxPerFilmPerMonth
                .flatMapToPair(
                        element -> {
                            List<Tuple2<String, Tuple3<Long, Long, String>>> res = new ArrayList<>();
                            long onlineBox = element._2().getOnlineBox();
                            long totalBox = element._2().getTotalBox();
                            String month = element._2().getTime();
                            for (int i = 0; i < element._2().getType().size(); i++) {
                                res.add(new Tuple2<>(element._2().getType().get(i), new Tuple3<>(onlineBox, totalBox, month)));
                            }
                            return res.iterator();
                        }
                ).reduceByKey(
                        (a, b) -> new Tuple3<>(a._1() + b._1(), a._2() + b._2(), a._3())
                );
//        boxPerTypePerMonth.print();
        boxPerTypePerMonth.foreachRDD(pairRdd -> {
            JavaRDD<Document> documents = pairRdd.map(t -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("type", t._1());
//                jsonObject.put("time", month.value());
                jsonObject.put("time", t._2()._3());
                jsonObject.put("total_month_box", t._2()._2());
                jsonObject.put("online_month_box", t._2()._1());
                Document document = Document.parse(jsonObject.toJSONString());
                return document;
            });
            MongoSpark.save(documents, writeConfig);
        });
    }

    private static void computeBoxPerFilm(JavaPairDStream<String, FilmStream> boxPerFilmPerMonth, JavaSparkContext sparkContext) {
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "boxPerFilm");
//        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(sparkContext).withOptions(writeOverrides);
//        writeConfig.withOption("collection", "boxPerFilm");
        JavaPairDStream<String, Tuple3<Long, Long, String>> boxPerFilm = boxPerFilmPerMonth
                .mapValues(v -> {
                    return new Tuple3<>(v.getOnlineBox(), v.getTotalBox(), v.getTime());
                }).updateStateByKey(
                        (currentValues, state) -> {
                            long online = 0l;
                            long total = 0l;
                            String month = currentValues.size() > 0 ? currentValues.get(0)._3() : "";
                            if (state.isPresent()) {
                                online = state.get()._1();
                                total = state.get()._2();
                            }
                            for (Tuple3<Long, Long, String> value : currentValues) {
                                online += value._1();
                                total += value._2();
                            }
                            return Optional.of(new Tuple3<>(online, total, month));
                        }
                );
//        boxPerFilm.print();
        boxPerFilm.foreachRDD(
                pairRdd -> {
                    JavaRDD<Document> documents = pairRdd.map(t -> {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("name", t._1());
                        jsonObject.put("time", t._2()._3());
                        jsonObject.put("total_box", t._2()._2());
                        jsonObject.put("online_box", t._2()._1());
                        Document document = Document.parse(jsonObject.toJSONString());
                        return document;
                    });
                    MongoSpark.save(documents, writeConfig);
                }
        );


    }

    private static JavaPairDStream<String, FilmStream> computeBoxPerFilmPerMonth(JavaPairDStream<String, FilmStream> perFilmPerMonth) {
        JavaPairDStream<String, FilmStream> boxPerFilmPerMonth = perFilmPerMonth
                .reduceByKey(
                        (a, b) -> {
                            FilmStream filmStream = new FilmStream(a);
                            filmStream.setOnlineBox(a.getOnlineBox() + b.getOnlineBox());
                            filmStream.setTotalBox(a.getTotalBox() + b.getTotalBox());
                            return filmStream;
                        }
                );
        return boxPerFilmPerMonth;
    }

    private static void receiveStream(JavaSparkContext sparkContext) {
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(1));
        JavaDStream<String> contentList = streamingContext.textFileStream("streamInput/");
        streamingContext.checkpoint("checkPoint/");
        JavaPairDStream<String, FilmStream> perFilmPerMonth = contentList
                .flatMap(content -> {
                    JSONArray jsonArray = JSONArray.parseArray(content);
                    List<FilmStream> res = new ArrayList<>();
                    for (int i = 0; i < jsonArray.size(); i++) {
                        res.add(JSONObject.parseObject(jsonArray.getString(i), FilmStream.class));
                    }
                    return res.iterator();
                }).filter(
                        filmStream -> {
                            return filmStream.getTotalBox() != null && filmStream.getOnlineBox() != null
                                    && filmStream.getOnlineBox() <= filmStream.getTotalBox();
                        }
                ).mapToPair(
                        filmStream -> {
                            filmStream.setTime(filmStream.getTime().substring(0, 7));
                            return new Tuple2<>(filmStream.getMovieName(), filmStream);
                        }
                );
////        Broadcast<String> month = sparkContext.broadcast(
////                new SimpleDateFormat("yyyy-MM").format(new Date(timeAccum.sum())));
        JavaPairDStream<String, FilmStream> boxPerFilmPerMonth = computeBoxPerFilmPerMonth(perFilmPerMonth);
//        JavaPairDStream<String, FilmStream> boxPerFilmPerMonth = computeBoxPerFilmPerMonth(perFilmPerMonth,month);
        computeBoxPerFilm(boxPerFilmPerMonth, sparkContext);
        computeBoxPerTypePerMonth(boxPerFilmPerMonth, sparkContext);
//        computeLocationRatePerMonth(boxPerFilmPerMonth, sparkContext);
//        computeActorPerYear(boxPerFilmPerMonth,sparkContext);
        streamingContext.start();              // Start the computation
        try {
            streamingContext.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
//            sparkContext.close();
        }

    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession sparkSession = SparkSession.builder()
                .appName("StreamingProcess")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/")
                .config("spark.mongodb.output.database", "sparkpractise")
                .config("spark.mongodb.output.collection", "testCollection")
                .getOrCreate();
//        SparkConf sparkConf = new SparkConf().setAppName("myStreaming");
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        receiveStream(sparkContext);
    }
}
