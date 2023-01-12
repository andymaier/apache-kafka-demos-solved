package de.predic8.j_serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class ArtikelSerde implements Serializer<Artikel>, Deserializer<Artikel>
{

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> map, boolean b) {
        System.out.println("map = " + map);
        System.out.println(b);
    }

    @Override
    public Artikel deserialize(String s, byte[] bytes) {
        try {
            return mapper.readerFor(Artikel.class).readValue(bytes);
        } catch (IOException e) {
            throw new RuntimeException("", e);
        }
    }

    @Override
    public byte[] serialize(String s, Artikel artikel) {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();) {
            mapper.writerFor(Artikel.class).writeValue(baos, artikel);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("", e);
        }
    }

    @Override
    public void close() {
        mapper = null;
    }
}
