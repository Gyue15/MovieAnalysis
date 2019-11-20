package cluster;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;

public class LDACluster {

    public static void train(JavaPairRDD<Long, Vector> corpus, JavaSparkContext jsc, int topicNum) {
        // Cluster the documents into three topics using LDA
        LDAModel ldaModel = new LDA().setK(topicNum).run(corpus);

        // Output topics. Each is a distribution over words (matching word count vectors)
        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize() + " words):");
        Matrix topics = ldaModel.topicsMatrix();
        for (int topic = 0; topic < topicNum; topic++) {
            System.out.print("Topic " + topic + ":");
            for (int word = 0; word < ldaModel.vocabSize(); word++) {
                System.out.print(" " + topics.apply(word, topic));
            }
            System.out.println();
        }

        ldaModel.save(jsc.sc(), "target/model/LatentDirichletAllocation");
    }

//    public static void predict()
}
