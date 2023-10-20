package org.thesis.utils;

import org.apache.beam.sdk.io.FileSystems;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;

public class Utils {
    Utils() {

    }

    public static String getSchema(String schemaPath) throws IOException {
        ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
                schemaPath, false));

        try (InputStream stream = Channels.newInputStream(chan)) {
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            StringBuilder dataBuilder = new StringBuilder();

            String line;
            while ((line = streamReader.readLine()) != null) {
                dataBuilder.append(line);
            }

            return dataBuilder.toString();
        }
    }

}
