package org.apache.flink.streaming.connectors.redis.common.mapper;

/** All available data type for Redis. */
public enum RedisDataType {

    /**
     * Strings are the most basic kind of Redis value. Redis Strings are binary safe, this means
     * that a Redis string can contain any kind of data, for instance a JPEG image or a serialized
     * Ruby object. A String value can be at max 512 Megabytes in length.
     */
    STRING,

    /** Redis Hashes are maps between string fields and string values. */
    HASH,

    HINCRBY,

    /** Redis Lists are simply lists of strings, sorted by insertion order. */
    LIST,

    /** Redis Sets are an unordered collection of Strings. */
    SET,

    /**
     * Redis Sorted Sets are, similarly to Redis Sets, non repeating collections of Strings. The
     * difference is that every member of a Sorted Set is associated with score, that is used in
     * order to take the sorted set ordered, from the smallest to the greatest score. While members
     * are unique, scores may be repeated.
     */
    SORTED_SET,

    /** HyperLogLog is a probabilistic data structure used in order to count unique things. */
    HYPER_LOG_LOG,

    /**
     * Redis implementation of publish and subscribe paradigm. Published messages are characterized
     * into channels, without knowledge of what (if any) subscribers there may be. Subscribers
     * express interest in one or more channels, and only receive messages that are of interest,
     * without knowledge of what (if any) publishers there are.
     */
    PUBSUB
}
