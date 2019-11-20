import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SparkSession;
import regression.GBTRegression;


public class Main {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("StreamingProcess")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/")
                .config("spark.mongodb.output.database", "film")
                .config("spark.mongodb.output.collection", "testCollection")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        Logger.getLogger("org").setLevel(Level.ERROR);
        String modelPath = "target/model/GradientBoostingRegression";
        JavaRDD<LabeledPoint> testData = DataLoader.loadLabeledPoint(jsc, "data/box_test_data.txt");
        JavaPairRDD<Double, Double> test = GBTRegression.test(testData, jsc, modelPath);
        System.out.println(test.collect());

    }
}
