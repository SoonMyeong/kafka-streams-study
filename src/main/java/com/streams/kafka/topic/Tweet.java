package com.streams.kafka.topic;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Getter
@Setter
public class Tweet {
    @JsonProperty("CreatedAt")
    private Long createdAt;

    @JsonProperty("Id")
    private Long id;

    @JsonProperty("Lang")
    private String lang;

    @JsonProperty("Retweet")
    private Boolean retweet;

    @JsonProperty("Text")
    private String text;

    public static Serde serdes() {
        JsonSerializer serializer = new JsonSerializer<>();
        JsonDeserializer deserializer = new JsonDeserializer<>(Tweet.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public boolean isRetweet() {
        return this.retweet;
    }

}
