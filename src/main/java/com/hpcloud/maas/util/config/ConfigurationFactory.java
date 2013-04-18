package com.hpcloud.maas.util.config;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TreeTraversingParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.hpcloud.maas.util.validation.Validator;
import com.yammer.dropwizard.config.ConfigurationException;
import com.yammer.dropwizard.json.ObjectMapperFactory;

public class ConfigurationFactory<T> {
  private final Class<T> klass;
  private final ObjectMapper mapper;

  private ConfigurationFactory(Class<T> klass) {
    this.klass = klass;
    ObjectMapperFactory objectMapperFactory = new ObjectMapperFactory();
    objectMapperFactory.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    this.mapper = objectMapperFactory.build(new YAMLFactory());
  }

  public static <T> ConfigurationFactory<T> forClass(Class<T> klass) {
    return new ConfigurationFactory<T>(klass);
  }

  public T build(File file) throws IOException, ConfigurationException {
    final JsonNode node = mapper.readTree(file);
    final String filename = file.toString();
    return build(node, filename);
  }

  private T build(JsonNode node, String filename) throws IOException, ConfigurationException {
    T config = mapper.readValue(new TreeTraversingParser(node), klass);
    validate(filename, config);
    return config;
  }

  private void validate(String file, T config) throws ConfigurationException {
    final ImmutableList<String> errors = new Validator().validate(config);
    if (!errors.isEmpty()) {
      throw new ConfigurationException(file, errors);
    }
  }
}
