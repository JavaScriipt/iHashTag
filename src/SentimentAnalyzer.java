package iHashTag;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SentimentAnalyzer {
    
    public TweetWithSentiment findSentiment(String line, StanfordCoreNLP stanfordCoreNLP) {

        int mainSentiment = 0;
        if (line != null && !line.isEmpty()) {
            int longest = 0;
            Annotation annotation = stanfordCoreNLP.process(line);
            for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
            }
        }
        //System.out.println(mainSentiment);
        if (mainSentiment == 2 || mainSentiment > 4 || mainSentiment < 0) {
            //return null;
        }
        TweetWithSentiment tweetWithSentiment = new TweetWithSentiment(line, toCss(mainSentiment));
        return tweetWithSentiment;
    }

    private String toCss(int sentiment) {
    	//System.out.println(sentiment);
        switch (sentiment) {
            case 0:
                //return "Very Negative";
            	return "-2";
            case 1:
                //return "Negative";
            	return "-1";
            case 2:
                //return "Neutral";
            	return "0";
            case 3:
                //return "Positive";
            	return "1";
            case 4:
                //return "Very Positive";
            	return "2";
            default:
                return "0";
        }
    }
}
