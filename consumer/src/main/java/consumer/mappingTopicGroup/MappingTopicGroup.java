package consumer.mappingTopicGroup;

import java.util.HashMap;
import java.util.Map;

public class MappingTopicGroup {//홈 매핑 클래스.
	
	 // Define your mapping data
    private Map<String, String> topicToGroupMapping;

    // Constructor to initialize data
    public MappingTopicGroup() {
    	topicToGroupMapping = new HashMap<>();
//    	topicToGroupMapping.put("from_ucrm_citwrlscntrtsms_message", "cg_ucrm_from_ucrm_citwrlscntrtsms_message");
//    	topicToGroupMapping.put("from_ucrm_citcablcntrtsms_message", "cg_ucrm_from_ucrm_citcablcntrtsms_message");
    	topicToGroupMapping.put("thirdtopic", "firsttopic");
    	topicToGroupMapping.put("forthtopic", "secondtopic");
    	
    }

    public String getGroupBytopic(String id) {
        return topicToGroupMapping.get(id);
    }

}
