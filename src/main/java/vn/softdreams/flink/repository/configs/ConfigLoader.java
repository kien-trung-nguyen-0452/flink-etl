package vn.softdreams.flink.repository.configs;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;

public final class ConfigLoader {
    private ConfigLoader() {
    }
    public static JobConfig load(String resource) {
        Yaml yaml = new Yaml(new Constructor(JobConfig.class));
        try (InputStream is = ConfigLoader.class
                .getClassLoader()
                .getResourceAsStream(resource)) {
            if (is == null) {
                throw new IllegalArgumentException(
                        "Config file not found: " + resource
                );
            }
            return yaml.load(is);

        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot load config: " + resource,
                    e
            );
        }
    }
}