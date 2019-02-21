package com.takzok.kafka.util;

import java.io.IOException;
import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

public class PropertyUtil {
    private static String PROPERTY_FILE;
    
    public PropertyUtil() {
    }
    
    public PropertyUtil(String propertyFile) {
        PROPERTY_FILE = propertyFile;
    }
    public Properties readProperty() throws IOException {
        Properties properties = new Properties();
            properties.load(Files.newBufferedReader(Paths.get(PROPERTY_FILE), StandardCharsets.UTF_8));
        return properties;
    }
}