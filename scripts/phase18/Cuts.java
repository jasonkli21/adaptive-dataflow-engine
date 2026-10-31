// Read-only source-cut fixture: actual earliest/latest offsets, never group offsets.
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
public class Cuts {
  public static void main(String[] args) throws Exception {
    Properties p=new Properties();p.put("bootstrap.servers",args[0]);p.put("request.timeout.ms","3000");p.put("default.api.timeout.ms","5000");
    try(Admin admin=Admin.create(p)) {
      var topic=admin.describeTopics(List.of(args[1])).allTopicNames().get(6,TimeUnit.SECONDS).get(args[1]);
      Map<TopicPartition,OffsetSpec> low=new HashMap<>(),high=new HashMap<>();
      for(var part:topic.partitions()) {var key=new TopicPartition(args[1],part.partition());low.put(key,OffsetSpec.earliest());high.put(key,OffsetSpec.latest());}
      var lows=admin.listOffsets(low).all().get(6,TimeUnit.SECONDS);var highs=admin.listOffsets(high).all().get(6,TimeUnit.SECONDS);
      for(var part:topic.partitions()) {var key=new TopicPartition(args[1],part.partition());System.out.println(part.partition()+"\t"+lows.get(key).offset()+"\t"+highs.get(key).offset());}
    }
  }
}
