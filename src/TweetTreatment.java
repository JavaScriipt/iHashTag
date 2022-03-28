package iHashTag;

import com.vdurmont.emoji.EmojiParser;

public class TweetTreatment {
	
	public String tweet_cleaner(String dirty_tweet) {
		dirty_tweet = dirty_tweet.replace("\""," ");
		dirty_tweet = dirty_tweet.replace("\n", " ").replace("\r", " ");
		dirty_tweet = EmojiParser.removeAllEmojis(dirty_tweet);
		System.out.println(dirty_tweet);
		String clean_tweet = "";
		String parse[] = dirty_tweet.split(" ");
		int index = 0;
		
		for (String element : parse) {
			//System.out.println(element);
			if(element.startsWith("#")){
				parse[index] = "@CLEAN@";
			}else if(element.contains("\\n")){
				parse[index] = element.replace("\\n", " ");
				if(parse[index].startsWith("http")) {
					parse[index] = "@CLEAN@";
				}
			}else if(element.startsWith("http")) {
				parse[index] = "@CLEAN@";
			}

			index++;
		}
		
		for (String clean_element : parse) {
			if(!clean_element.contentEquals("@CLEAN@")) {
				clean_tweet+=clean_element+" ";
			}
		}

		clean_tweet.replaceAll("[^\\dA-Za-z' '.,;']", "");
		
		System.out.println("TWETT LIMPIO: " +clean_tweet);
		return clean_tweet;
	}
}
