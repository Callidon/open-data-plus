package com.alma.opendata.classifier;

import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.core.SerializationHelper;
import weka.filters.Filter;
import weka.filters.unsupervised.instance.Randomize;

import java.io.FileReader;

/**
 * WekaClassifier based on the official weka example
 * Use J48 tree to classify the Fisher's Iris dataset (https://en.wikipedia.org/wiki/Iris_flower_data_set)
 */
public class WekaClassifier {

    /** the classifier used internally */
    private weka.classifiers.Classifier classifier;

    /** the filter to use */
    private Filter filter;

    /** the training file */
    private String trainingFile;

    /** the training instances */
    private Instances trainingInstances;

    /** for evaluating the classifier */
    private Evaluation evaluation;
    private Instances filtered;

    public WekaClassifier() {
        classifier = new J48();
        filter = new Randomize();
    }

    /**
     * Set the set to use for training
     */
    public void setTrainingSet(String name) throws Exception {
        trainingFile = name;
        trainingInstances = new Instances(new FileReader(name));
        trainingInstances.setClassIndex(trainingInstances.numAttributes() - 1);
        // run filter to separate train & test instances form reference set
        filter.setInputFormat(trainingInstances);
        filtered = Filter.useFilter(trainingInstances, filter);
    }

    /**
     * Train the classifier with the provided instances
     * @throws Exception
     */
    public void train() throws Exception {
        // train classifier on complete file for tree
        classifier.buildClassifier(filtered);
    }

    /**
     * Run 10fold CV over the training file
     */
    public void execute() throws Exception {
        // 10fold CV with a seed of 1
        evaluation = new Evaluation(filtered);
        evaluation.crossValidateModel(classifier, filtered, 10, trainingInstances.getRandomNumberGenerator(1));
    }

    /**
     * Export the trained classifier in a file
     * @param outputModel
     * @throws Exception
     */
    public void exportClassifier(String outputModel) throws Exception {
        SerializationHelper.write(outputModel, classifier);
    }

    /**
     * Import a classifier from a file
     * @param modelPath
     * @throws Exception
     */
    public void importClassifier(String modelPath) throws Exception {
        classifier = (weka.classifiers.Classifier) SerializationHelper.read(modelPath);
    }

    /**
     * Output some data about the classifier
     */
    public String show() throws Exception {
        return "WekaClassifier...: " + classifier.getClass().getName() + "\n" +
                "Filter.......: " + filter.getClass().getName() + "\n" +
                "Training file: " + trainingFile + "\n" +
                classifier.toString() + "\n" +
                "===========\n" + "Evaluation : \n===========\n" +
                evaluation.toSummaryString() + "\n" +
                evaluation.toMatrixString() + "\n" +
                evaluation.toClassDetailsString() + "\n";
    }

    public static void main(String[] args) throws Exception {
        WekaClassifier demo = new WekaClassifier();
        demo.setTrainingSet("src/main/resources/iris.arff");
        demo.train();
        demo.exportClassifier("src/main/resources/iris.model");
        demo.importClassifier("src/main/resources/iris.model");
        demo.execute();
        System.out.println(demo.show());
    }
}
