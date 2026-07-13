package vn.softdreams.flink.repository.configs;

public final class CommandLineOptions {

    public static final String DEFAULT_CONFIG =
            "config/repository-ledger.yml";

    public static final String CONFIG_PATH_ENV = "CONFIG_PATH";

    private final String configPath;

    private CommandLineOptions(String configPath) {
        this.configPath = configPath;
    }

    public String getConfigPath() {
        return configPath;
    }

    /**
     * Resolves config path with priority: --config CLI arg &gt; CONFIG_PATH env &gt; default.
     */
    public static CommandLineOptions parse(String[] args) {

        String config = resolveDefaultConfigPath();

        if (args != null) {
            for (int i = 0; i < args.length; i++) {

                switch (args[i]) {

                    case "--config":
                        if (i + 1 >= args.length) {
                            throw new IllegalArgumentException(
                                    "--config requires a value"
                            );
                        }
                        config = args[++i];
                        break;

                    default:
                        throw new IllegalArgumentException(
                                "Unknown argument: " + args[i]
                        );
                }
            }
        }

        return new CommandLineOptions(config);
    }

    private static String resolveDefaultConfigPath() {
        String envConfig = System.getenv(CONFIG_PATH_ENV);
        if (envConfig != null && !envConfig.isBlank()) {
            return envConfig.trim();
        }
        return DEFAULT_CONFIG;
    }

}