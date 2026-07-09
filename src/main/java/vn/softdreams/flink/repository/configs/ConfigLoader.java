package vn.softdreams.flink.repository.configs;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.LoaderOptions;

import java.io.InputStream;

public final class ConfigLoader {

    private ConfigLoader() {}
    public static JobConfig load(String resource) {
        LoaderOptions opts = new LoaderOptions();
        Yaml yaml = new Yaml(new Constructor(JobConfig.class, opts));
        try (InputStream is = ConfigLoader.class
                .getClassLoader()
                .getResourceAsStream(resource)) {
            if (is == null) {
                throw new IllegalArgumentException(
                        "Config file not found in classpath: " + resource
                );
            }
            return yaml.load(is);
        } catch (Exception e) {
            throw new RuntimeException("Cannot load config: " + resource, e);
        }
    }
}