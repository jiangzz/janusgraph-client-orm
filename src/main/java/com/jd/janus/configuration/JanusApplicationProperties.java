package com.jd.janus.configuration;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.net.URL;

@Slf4j
public class JanusApplicationProperties extends PropertiesConfiguration {
    private static volatile Configuration instance = null;
    private static volatile String defaultName="jtlas-application.properties";

    private JanusApplicationProperties(URL url) throws ConfigurationException {
        super(url);
    }

    @SneakyThrows
    public static Configuration get(){
        if (instance == null) {
            synchronized (JanusApplicationProperties.class) {
                if (instance == null) {
                    URL resource = JanusApplicationProperties.class.getClassLoader().getResource(defaultName);
                    instance = new JanusApplicationProperties(resource);
                }
            }
        }
        return instance;
    }
}
