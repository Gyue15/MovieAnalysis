package regression;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class GBTRegression {

    private static String modelPath = "target/model/GradientBoostingRegression";

    public static void train(JavaRDD<LabeledPoint> trainingData, JavaSparkContext jsc, int numIter, int maxDepth,
                             String path) {
        // Train a GradientBoostedTrees model.
        // The defaultParams for Regression use SquaredError by default.
        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
        boostingStrategy.setNumIterations(numIter); // Note: Use more iterations in practice.
        boostingStrategy.getTreeStrategy().setMaxDepth(maxDepth);
        // Empty categoricalFeaturesInfo indicates all features are continuous.
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

        GradientBoostedTreesModel model = GradientBoostedTrees.train(trainingData, boostingStrategy);
        model.save(jsc.sc(), path == null? modelPath: path);
        System.out.println("Learned regression GBT model:\n" + model.toDebugString());
    }

    public static JavaPairRDD<Double, Double> test(JavaRDD<LabeledPoint> testData, JavaSparkContext jsc, String path) {
        GradientBoostedTreesModel model = GradientBoostedTreesModel.load(jsc.sc(), path == null? modelPath: path);
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
        double testMSE = predictionAndLabel.mapToDouble(pl -> {
            double diff = pl._1() - pl._2();
            return diff * diff;
        }).mean();
        System.out.println("Test Mean Squared Error: " + testMSE);
//        System.out.println("Learned regression GBT model:\n" + model.toDebugString());
        return predictionAndLabel;
    }
}
