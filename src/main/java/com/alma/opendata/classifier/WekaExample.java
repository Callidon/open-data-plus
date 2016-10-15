package com.alma.opendata.classifier;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.unsupervised.instance.Randomize;

import java.io.FileReader;
import java.io.IOException;

/**
 * Classifier based on the official weka example
 * Use J48 tree to classify the Fisher's Iris dataset (https://en.wikipedia.org/wiki/Iris_flower_data_set)
 */
public class WekaExample {

    /** the classifier used internally */
    private Classifier classifier;

    /** the filter to use */
    private Filter filter;

    /** the training file */
    private String trainingFile;

    /** the training instances */
    private Instances trainingInstances;

    /** for evaluating the classifier */
    private Evaluation evaluation;

    public WekaExample() {
        classifier = new J48();
        filter = new Randomize();
    }

    /**
     * sets the file to use for training
     */
    public void setTraining(String name) {
        trainingFile = name;
        try {
            trainingInstances = new Instances(new FileReader(name));
            trainingInstances.setClassIndex(trainingInstances.numAttributes() - 1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * runs 10fold CV over the training file
     */
    public void execute() throws Exception {
        // run filter
        filter.setInputFormat(trainingInstances);
        Instances filtered = Filter.useFilter(trainingInstances, filter);

        // train classifier on complete file for tree
        classifier.buildClassifier(filtered);

        // 10fold CV with a seed of 1
        evaluation = new Evaluation(filtered);
        evaluation.crossValidateModel(classifier, filtered, 10, trainingInstances.getRandomNumberGenerator(1));
    }

    /**
     * outputs some data about the classifier
     */
    public String show() {
        StringBuilder result = new StringBuilder();

        result.append("Weka - Demo\n===========\n\n");
        result.append("Classifier...: ").append(classifier.getClass().getName()).append("\n");
        result.append("Filter.......: ").append(filter.getClass().getName()).append("\n");
        result.append("Training file: ").append(trainingFile).append("\n");
        result.append(classifier.toString()).append("\n");
        result.append(evaluation.toSummaryString()).append("\n");
        try {
            result.append(evaluation.toMatrixString()).append("\n");
            result.append(evaluation.toClassDetailsString()).append("\n");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result.toString();
    }

    public static void main(String[] args) throws Exception {
        WekaExample demo = new WekaExample();
        demo.setTraining("src/main/resources/iris.arff");
        demo.execute();
        System.out.println(demo.show());
    }
}
