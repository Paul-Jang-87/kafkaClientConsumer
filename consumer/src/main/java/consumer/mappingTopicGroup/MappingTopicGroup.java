package consumer.mappingTopicGroup;

import java.util.HashMap;
import java.util.Map;

public class MappingTopicGroup {//홈 매핑 클래스.
	
	 // Define your mapping data
    private Map<String, String> topicToGroupMapping;

    // Constructor to initialize data
    public MappingTopicGroup() {
    	topicToGroupMapping = new HashMap<>();
    	topicToGroupMapping.put("from_ucrm_ctiwrlscntrtsms_message", "cg_clcc_from_ucrm_ctiwrlscntrtsms_message");
    	topicToGroupMapping.put("from_ucrm_cticablcntrtsms_message", "cg_clcc_from_ucrm_cticablcntrtsms_message");
    	topicToGroupMapping.put("from_cscallbot_cmpnhmitem_message", "cg_clcc_from_cscallbot_cmpnhmitem_message");
    	topicToGroupMapping.put("from_cscallbot_cmpnmblitem_message", "cg_clcc_from_cscallbot_cmpnmblitem_message");
    	
    }

    public String getGroupBytopic(String id) {
        return topicToGroupMapping.get(id);
    }

}
