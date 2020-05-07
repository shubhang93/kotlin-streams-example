import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KTable
import java.util.*

const val topicName = "wordCount"

fun createWordCountStream(builder: StreamsBuilder) {
    val words = builder.stream<String, String>(topicName)
    val wordCounts: KTable<String, Long> = words.filter { key, value -> key.startsWith("H", true) }
        .groupBy { key, value -> key }
        .count()

    println(wordCounts.toStream().foreach { k, v -> println("K: $k");println("V: $v") })
}

fun main() {

    val streamsConfig = Properties()
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-kotlin");
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name);
    streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
    streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    val streamsBuilder = StreamsBuilder()
    createWordCountStream(streamsBuilder)
    val kafkaStreams = KafkaStreams(streamsBuilder.build(), streamsConfig)
    kafkaStreams.start()
    Runtime.getRuntime().addShutdownHook(Thread { kafkaStreams.close() });

}