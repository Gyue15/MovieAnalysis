package regression;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

public class LinearRegression {

    private static String modelPath = "target/model/LinearRegressionWithSGDModel";

    private static double computeError( JavaRDD<LabeledPoint> testData, LinearRegressionModel model) {
        JavaPairRDD<Double, Double> valuesAndPredicts =
                testData.mapToPair(point -> new Tuple2<>(model.predict(point.features()), point.label()));
        return valuesAndPredicts.mapToDouble(pair -> {
            double diff = pair._1() - pair._2();
            return diff * diff;
        }).mean();
    }

    public static void train(JavaRDD<LabeledPoint> trainingData, JavaSparkContext jsc, int numIter, double stepSize,
                             String path) {
        LinearRegressionModel model = LinearRegressionWithSGD.train(JavaRDD.toRDD(trainingData), numIter, stepSize);
        model.save(jsc.sc(), path == null? modelPath: path);
        double trainError = computeError(trainingData, model);
        System.out.println("train Mean Error = " + trainError);
    }

    public static void test(JavaRDD<LabeledPoint> testData, JavaSparkContext jsc, String path) {
        LinearRegressionModel model = LinearRegressionModel.load(jsc.sc(), path == null? modelPath: path);
        double testError = computeError(testData, model);
        System.out.println("test Mean Error = " + testError);
    }

    public static JavaPairRDD<Double, Double> predict(JavaRDD<LabeledPoint> testData, JavaSparkContext jsc, String path) {
        LinearRegressionModel model = LinearRegressionModel.load(jsc.sc(), path == null? modelPath: path);
        return testData.mapToPair(point -> new Tuple2<>(model.predict(point.features()), point.label()));
    }
}
