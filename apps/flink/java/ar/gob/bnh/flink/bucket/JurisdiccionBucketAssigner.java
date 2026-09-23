package ar.gob.bnh.flink.bucket;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JurisdiccionBucketAssigner
        implements BucketAssigner<String, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String getBucketId(String element, Context context) {
        try {
            JsonNode root = MAPPER.readTree(element);

            String jurisdiccion = root
                    .path("metadata")
                    .path("jurisdiccion")
                    .asText("")
                    .trim();

            if (jurisdiccion.isEmpty()) {
                return "_SIN_JURISDICCION";
            }

            String safe = jurisdiccion.replaceAll(
                    "[^A-Za-z0-9._-]",
                    "_"
            );

            if (safe.isEmpty() || safe.equals(".") || safe.equals("..")) {
                return "_SIN_JURISDICCION";
            }

            return safe;

        } catch (Exception e) {
            return "_SIN_JURISDICCION";
        }
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
