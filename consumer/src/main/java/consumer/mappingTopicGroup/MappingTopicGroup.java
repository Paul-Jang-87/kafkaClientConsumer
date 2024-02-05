package consumer.mappingTopicGroup;

import java.util.HashMap;
import java.util.Map;

public class MappingTopicGroup {//홈 매핑 클래스.
	
	 // Define your mapping data
    private Map<String, String> topicToGroupMapping;

    // Constructor to initialize data
    public MappingTopicGroup() { 
    	topicToGroupMapping = new HashMap<>();
    	topicToGroupMapping.put("firsttopic", "firstgroup");
    	topicToGroupMapping.put("secondtopic", "secondgroup");
    	topicToGroupMapping.put("thirdtopic", "thirdgroup");
    	topicToGroupMapping.put("forthtopic", "forthgroup");
    	topicToGroupMapping.put("fifthtopic", "fifthgroup");
    	topicToGroupMapping.put("sixthtopic", "sixthgroup");
    	
    	topicToGroupMapping.put("callbotfirst", "callbotgroup1");
    	topicToGroupMapping.put("callbotsecond", "callbotgroup2");
    	topicToGroupMapping.put("callbotthird", "callbotgroup3");
    	topicToGroupMapping.put("callbotforth", "callbotgroup4");
    	topicToGroupMapping.put("callbotfifth", "callbotgroup5");
    	topicToGroupMapping.put("callbotsixth", "callbotgroup6");
    }

    public String getGroupBytopic(String id) {
        return topicToGroupMapping.get(id);
    }

}
