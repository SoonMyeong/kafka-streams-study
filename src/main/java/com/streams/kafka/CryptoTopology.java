package com.streams.kafka;

import com.streams.kafka.topic.Tweet;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CryptoTopology {

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<byte[], Tweet> stream =
                streamsBuilder.stream("tweets", Consumed.with(Serdes.ByteArray(), Tweet.serdes()));
        KStream<byte[], Tweet> filtered =
                stream.filterNot(
                        (key, tweet) -> tweet.isRetweet());

        filtered.to(
                "filter-res",
                Produced.with(
                        Serdes.ByteArray(),
                        com.mitchseymour.kafka.serialization.avro.AvroSerdes.get(FilterRes.class)));
    }
}
