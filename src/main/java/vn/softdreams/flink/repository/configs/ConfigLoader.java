package vn.softdreams.flink.repository.configs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ConfigLoader {

    private static final Logger LOG =
            LoggerFactory.getLogger(ConfigLoader.class);

    private ConfigLoader() {
    }

    public static JobConfig load(String configPath) {

        LoaderOptions options = new LoaderOptions();
        Yaml yaml = new Yaml(new Constructor(JobConfig.class, options));

        Path file = Path.of(configPath);
        if (Files.exists(file) && Files.isRegularFile(file)) {

            LOG.info("Loading config from file: {}", file.toAbsolutePath());

            try (InputStream is = Files.newInputStream(file)) {
                return yaml.load(is);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Cannot read config file: " + file.toAbsolutePath(),
                        e
                );
            }
        }
        LOG.info("Config file [{}] not found on filesystem. Trying classpath...", configPath);

        try (InputStream is = ConfigLoader.class
                .getClassLoader()
                .getResourceAsStream(configPath)) {

            if (is == null) {
                throw new IllegalArgumentException(
                        "Config not found: " + configPath
                );
            }

            LOG.info("Loaded config from classpath: {}", configPath);

            return yaml.load(is);

        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot load config from classpath: " + configPath,
                    e
            );
        }
    }
}