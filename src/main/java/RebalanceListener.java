import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class RebalanceListener implements ConsumerRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private final static String GROUP_ID = "test-group";
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>(); //현재 처리한 오프셋을 매번 커밋하기 위해 commitSync() 메서드가 파라미터로 받을 HashMap 타입을 선언해야 한다.
            //HashMap의 키는 토픽과 파티션 정보가 담긴 TopicPartition 클래스가 되고 값은 오프셋 정보가 담긴 OffsetAndMetadata 클래스가 된다.

            for(ConsumerRecord<String, String> record : records){
                logger.info("record:{}", record);
                currentOffset.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null)); //처리를 완료한 레코드의 정보를 토대로 Map<TopicPartition, OffsetAndMetadata>인스턴스에 키/값을 넣는다.
                //이때 주의할 점은 현재 처리한 오프셋에 1을 더한 값을 커밋해야 한다는 점이다. 이후에 컨슈머가 poll()을 수행할 때 마지막으로 커밋한 오프셋부터 레코드를 리턴하기 때문이다.
                consumer.commitSync(currentOffset); //TopicPartition과 OffsetAndMetadata로 이루어진 HashMap을 commitSync() 메서드의 파라미터로 넣어 호출하면 해당 특정 토픽, 파티션의 오프셋이 매번 커밋된다.
            }
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.warn("Partitions are assigned : " + partitions.toString());

    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.warn("Partitions are revoked : " + partitions.toString());
    }
}
