use std::{
    env,
    ffi::OsString,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use config::{Config, Environment, File, FileFormat, Map};

use super::{AppConfig, schema::RawAppConfig};

const DEFAULT_CONFIG_PATH: &str = "config.toml";

impl AppConfig {
    /// Resolves, merges, deserializes, and validates the startup configuration.
    ///
    /// Source precedence, from lowest to highest, is compiled defaults, an
    /// optional TOML file, then FLUX__ environment variables. A CLI path takes
    /// precedence over `FLUX_CONFIG`; either is required when selected. The
    /// default ./config.toml is used only when it exists.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicit file is missing, a source cannot be
    /// parsed, the merged schema contains unknown or invalid fields, required
    /// Kafka settings are absent, or validation fails.
    pub fn load(cli_path: Option<PathBuf>) -> Result<Self> {
        let file_path = resolve_config_file(
            cli_path,
            env::var_os("FLUX_CONFIG"),
            PathBuf::from(DEFAULT_CONFIG_PATH),
        )?;
        load_from_sources(file_path.as_deref(), flux_environment(None))
    }
}

fn resolve_config_file(
    cli_path: Option<PathBuf>,
    environment_path: Option<OsString>,
    default_path: PathBuf,
) -> Result<Option<PathBuf>> {
    if let Some(path) = cli_path {
        return Ok(Some(path));
    }
    if let Some(path) = environment_path {
        return Ok(Some(PathBuf::from(path)));
    }
    if default_path.try_exists().with_context(|| {
        format!(
            "failed to inspect default config {}",
            default_path.display()
        )
    })? {
        return Ok(Some(default_path));
    }
    Ok(None)
}

fn load_from_sources(file_path: Option<&Path>, environment: Environment) -> Result<AppConfig> {
    let mut builder = Config::builder();
    if let Some(path) = file_path {
        builder = builder.add_source(File::from(path).format(FileFormat::Toml).required(true));
    }
    let merged = builder
        .add_source(environment)
        .build()
        .with_context(|| source_error_context(file_path))?;
    let raw = merged
        .try_deserialize::<RawAppConfig>()
        .context("failed to deserialize merged configuration")?;
    let config = AppConfig::from(raw);
    config.validate()?;
    Ok(config)
}

fn source_error_context(file_path: Option<&Path>) -> String {
    file_path.map_or_else(
        || "failed to merge environment configuration".to_owned(),
        |path| format!("failed to merge config sources using {}", path.display()),
    )
}

fn flux_environment(source: Option<Map<String, String>>) -> Environment {
    let environment = Environment::with_prefix("FLUX")
        .prefix_separator("__")
        .separator("__");
    match source {
        Some(source) => environment.source(Some(source)),
        None => environment,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    static NEXT_TEMP_FILE: AtomicUsize = AtomicUsize::new(0);

    const MINIMAL_TOML: &str = r#"
        [kafka]
        bootstrap_servers = ["file-broker:9092"]
        group_id = "file-group"
        topics = ["file-topic"]
        auto_offset_reset = "earliest"
    "#;

    struct TestConfigFile {
        path: PathBuf,
    }

    impl TestConfigFile {
        fn new(contents: &str) -> Self {
            let sequence = NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "flux-configuration-v2-{}-{sequence}.toml",
                std::process::id()
            ));
            fs::write(&path, contents).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestConfigFile {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    fn test_environment(entries: &[(&str, &str)]) -> Environment {
        let source = entries
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        flux_environment(Some(source))
    }

    fn required_environment() -> Map<String, String> {
        [
            (
                "FLUX__KAFKA__BOOTSTRAP_SERVERS".to_owned(),
                "env-broker-a:9092,env-broker-b:9092".to_owned(),
            ),
            ("FLUX__KAFKA__GROUP_ID".to_owned(), "env-group".to_owned()),
            (
                "FLUX__KAFKA__TOPICS".to_owned(),
                "signals-a,signals-b".to_owned(),
            ),
            (
                "FLUX__KAFKA__AUTO_OFFSET_RESET".to_owned(),
                "earliest".to_owned(),
            ),
        ]
        .into_iter()
        .collect()
    }

    fn load_test(file_path: Option<&Path>, source: Map<String, String>) -> Result<AppConfig> {
        load_from_sources(file_path, flux_environment(Some(source)))
    }

    #[test]
    fn canonical_example_remains_valid_as_a_toml_only_source() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config.example.toml");
        let config = load_test(Some(&path), Map::new()).unwrap();

        assert_eq!(config.kafka.bootstrap_servers, ["localhost:9092"]);
        assert_eq!(config.ingress.work_queue_capacity, 2_048);
        assert_eq!(config.object_processing.worker_count, 8);
        assert_eq!(config.s3.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.shutdown.grace_ms, 30_000);
    }

    #[test]
    fn env_only_configuration_parses_required_fields_and_only_known_lists() {
        let mut source = required_environment();
        source.insert(
            "FLUX__S3__ENDPOINT_URL".to_owned(),
            "http://minio:9000".to_owned(),
        );
        source.insert("FLUX__KAFKA__GROUP_ID".to_owned(), "env,group".to_owned());
        source.insert("FLUX__S3__FORCE_PATH_STYLE".to_owned(), "true".to_owned());
        source.insert("FLUX__S3__MAX_ATTEMPTS".to_owned(), "4".to_owned());
        let config = load_test(None, source).unwrap();

        assert_eq!(
            config.kafka.bootstrap_servers,
            ["env-broker-a:9092", "env-broker-b:9092"]
        );
        assert_eq!(config.kafka.topics, ["signals-a", "signals-b"]);
        assert_eq!(config.kafka.group_id, "env,group");
        assert_eq!(config.s3.endpoint_url.as_deref(), Some("http://minio:9000"));
        assert!(config.s3.force_path_style);
        assert_eq!(config.s3.max_attempts, 4);
        assert_eq!(config.ingress.work_queue_capacity, 2_048);
    }

    #[test]
    fn source_precedence_matches_the_complete_truth_table() {
        for (file_value, env_value, expected) in [
            (None, None, 2_048),
            (Some(17), None, 17),
            (None, Some(23), 23),
            (Some(17), Some(23), 23),
        ] {
            let file = TestConfigFile::new(&format!(
                "{MINIMAL_TOML}\n{}",
                file_value.map_or_else(String::new, |value| format!(
                    "[ingress]\nwork_queue_capacity = {value}"
                ))
            ));
            let source = env_value.map_or_else(Map::new, |value| {
                [(
                    "FLUX__INGRESS__WORK_QUEUE_CAPACITY".to_owned(),
                    value.to_string(),
                )]
                .into_iter()
                .collect()
            });

            let config = load_test(Some(file.path()), source).unwrap();

            assert_eq!(config.ingress.work_queue_capacity, expected);
        }
    }

    #[test]
    fn environment_lists_replace_file_lists() {
        let file = TestConfigFile::new(MINIMAL_TOML);
        let source = [
            (
                "FLUX__KAFKA__BOOTSTRAP_SERVERS".to_owned(),
                "broker-a:9092,broker-b:9092".to_owned(),
            ),
            (
                "FLUX__KAFKA__TOPICS".to_owned(),
                "topic-a,topic-b".to_owned(),
            ),
        ]
        .into_iter()
        .collect();

        let config = load_test(Some(file.path()), source).unwrap();

        assert_eq!(
            config.kafka.bootstrap_servers,
            ["broker-a:9092", "broker-b:9092"]
        );
        assert_eq!(config.kafka.topics, ["topic-a", "topic-b"]);
        assert_eq!(config.kafka.group_id, "file-group");
    }

    #[test]
    fn environment_lists_preserve_singleton_lexical_identity_and_trim_elements() {
        let file = TestConfigFile::new(MINIMAL_TOML);
        for (raw, expected) in [
            ("123", vec!["123"]),
            ("001", vec!["001"]),
            ("true", vec!["true"]),
            ("topic-a, topic-b", vec!["topic-a", "topic-b"]),
        ] {
            let source = [("FLUX__KAFKA__TOPICS".to_owned(), raw.to_owned())]
                .into_iter()
                .collect();

            let config = load_test(Some(file.path()), source).unwrap();

            assert_eq!(config.kafka.topics, expected, "{raw}");
        }

        let source = [(
            "FLUX__KAFKA__TOPICS".to_owned(),
            "topic-a,,topic-b".to_owned(),
        )]
        .into_iter()
        .collect();
        let error = load_test(Some(file.path()), source).unwrap_err();

        assert!(error.to_string().contains("non-empty topic"), "{error:#}");
    }

    #[test]
    fn cli_then_flux_config_then_optional_default_define_file_selection() {
        let cli = PathBuf::from("cli.toml");
        let environment = OsString::from("environment.toml");
        let default = PathBuf::from("does-not-exist.toml");

        assert_eq!(
            resolve_config_file(
                Some(cli.clone()),
                Some(environment.clone()),
                default.clone()
            )
            .unwrap(),
            Some(cli)
        );
        assert_eq!(
            resolve_config_file(None, Some(environment), default.clone()).unwrap(),
            Some(PathBuf::from("environment.toml"))
        );
        assert_eq!(resolve_config_file(None, None, default).unwrap(), None);

        let existing_default = TestConfigFile::new(MINIMAL_TOML);
        assert_eq!(
            resolve_config_file(None, None, existing_default.path().to_path_buf()).unwrap(),
            Some(existing_default.path().to_path_buf())
        );
    }

    #[test]
    fn an_explicit_missing_file_fails_but_an_absent_default_is_optional() {
        let missing = env::temp_dir().join(format!(
            "flux-missing-configuration-v2-{}-{}.toml",
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        assert_eq!(
            resolve_config_file(None, None, missing.clone()).unwrap(),
            None
        );

        let error = load_test(Some(&missing), required_environment()).unwrap_err();

        assert!(format!("{error:#}").contains(&missing.display().to_string()));
    }

    #[test]
    fn unknown_flux_keys_and_legacy_kafka_runtime_fields_are_rejected() {
        let mut source = required_environment();
        source.insert(
            "FLUX__KAFKA__WORK_QUEUE_CAPACITY".to_owned(),
            "17".to_owned(),
        );
        let environment_error = load_test(None, source).unwrap_err();
        assert!(
            format!("{environment_error:#}").contains("work_queue_capacity"),
            "{environment_error:#}"
        );

        let legacy = TestConfigFile::new(&format!("{MINIMAL_TOML}\nwork_queue_capacity = 17"));
        let file_error = load_test(Some(legacy.path()), Map::new()).unwrap_err();
        assert!(
            format!("{file_error:#}").contains("work_queue_capacity"),
            "{file_error:#}"
        );
    }

    #[test]
    fn invalid_bool_integer_and_enum_values_fail_deserialization() {
        for (key, value) in [
            ("FLUX__S3__FORCE_PATH_STYLE", "sometimes"),
            ("FLUX__INGRESS__WORK_QUEUE_CAPACITY", "many"),
            ("FLUX__KAFKA__AUTO_OFFSET_RESET", "middle"),
        ] {
            let mut source = required_environment();
            source.insert(key.to_owned(), value.to_owned());

            assert!(load_test(None, source).is_err(), "{key}={value}");
        }
    }

    #[test]
    fn kafka_required_fields_have_no_implicit_defaults() {
        let source = [
            ("FLUX__KAFKA__GROUP_ID".to_owned(), "env-group".to_owned()),
            (
                "FLUX__KAFKA__AUTO_OFFSET_RESET".to_owned(),
                "earliest".to_owned(),
            ),
        ]
        .into_iter()
        .collect();

        let error = load_test(None, source).unwrap_err();

        assert!(format!("{error:#}").contains("bootstrap_servers"));
    }

    #[test]
    fn aws_credentials_are_outside_app_config_and_flux_s3_secrets_are_forbidden() {
        let mut provider_environment = required_environment();
        provider_environment.insert("AWS_ACCESS_KEY_ID".to_owned(), "not-ingested".to_owned());
        provider_environment.insert(
            "AWS_SECRET_ACCESS_KEY".to_owned(),
            "not-ingested".to_owned(),
        );
        load_test(None, provider_environment).unwrap();

        let mut flux_secret = required_environment();
        flux_secret.insert(
            "FLUX__S3__ACCESS_KEY".to_owned(),
            "must-not-exist".to_owned(),
        );
        let error = load_test(None, flux_secret).unwrap_err();
        assert!(format!("{error:#}").contains("access_key"));
    }

    #[test]
    fn validation_runs_after_all_sources_are_merged() {
        let file = TestConfigFile::new(MINIMAL_TOML);
        for (entries, expected) in [
            (vec![("FLUX__SHUTDOWN__GRACE_MS", "0")], "shutdown.grace_ms"),
            (
                vec![("FLUX__OBJECT_PROCESSING__WORKER_COUNT", "0")],
                "worker_count",
            ),
            (
                vec![
                    ("FLUX__S3__CONNECT_TIMEOUT_MS", "5000"),
                    ("FLUX__S3__OPERATION_ATTEMPT_TIMEOUT_MS", "1000"),
                ],
                "connect_timeout_ms",
            ),
        ] {
            let source = entries
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect();

            let error = load_test(Some(file.path()), source).unwrap_err();

            assert!(error.to_string().contains(expected), "{error:#}");
        }
    }

    #[test]
    fn environment_source_can_be_injected_without_process_mutation() {
        let config = load_from_sources(
            None,
            test_environment(&[
                ("FLUX__KAFKA__BOOTSTRAP_SERVERS", "localhost:9092"),
                ("FLUX__KAFKA__GROUP_ID", "test"),
                ("FLUX__KAFKA__TOPICS", "signals"),
                ("FLUX__KAFKA__AUTO_OFFSET_RESET", "latest"),
            ]),
        )
        .unwrap();

        assert_eq!(
            config.kafka.auto_offset_reset,
            super::super::AutoOffsetReset::Latest
        );
    }
}
