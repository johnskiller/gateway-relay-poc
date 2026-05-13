use serde::Deserialize;

/// Gateway configuration loaded from JSON5 file.
///
/// Supports environment variable overrides for K8s deployment:
/// - `GATEWAY_ID`: Overrides the gateway ID (useful with Downward API)
/// - `GATEWAY_CONFIG`: Specifies config file path
///
/// Config file loading priority:
///   CLI --config <path>  >  GATEWAY_CONFIG env  >  /etc/zenoh-gateway/config.json5
///
/// Gateway ID resolution priority:
///   GATEWAY_ID env  >  CLI positional arg  >  config file id field  >  "gw-1"
#[derive(Debug, Deserialize, Clone)]
pub struct GatewayConfig {
    /// Gateway instance ID. Multiple gateways share the same ConfigMap,
    /// so this can be omitted and set via GATEWAY_ID environment variable.
    #[serde(default = "default_id")]
    pub id: String,

    /// Upstream zenoh session config (connect to Producer-side router)
    #[serde(default)]
    pub upstream: ZenohEndpoints,

    /// Downstream zenoh session config (connect to Consumer-side router)
    #[serde(default)]
    pub downstream: ZenohEndpoints,

    /// Key expression for cluster member discovery via Liveliness
    #[serde(default = "default_cluster_expr")]
    pub cluster_expr: String,

    /// Key expression for consumer Liveliness discovery
    #[serde(default = "default_consumer_liveliness_expr")]
    pub consumer_liveliness_expr: String,

    /// Statistics report interval in seconds
    #[serde(default = "default_stats_interval")]
    pub stats_interval_secs: u64,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ZenohEndpoints {
    /// Zenoh router endpoints to connect to (e.g., ["tcp/zenoh-upstream:7447"])
    #[serde(default)]
    pub connect: Vec<String>,

    /// Zenoh router endpoints to listen on (e.g., ["tcp/0.0.0.0:7447"])
    #[serde(default)]
    pub listen: Vec<String>,
}

fn default_id() -> String {
    "gw-1".to_string()
}

fn default_cluster_expr() -> String {
    "gateway/cluster/**".to_string()
}

fn default_consumer_liveliness_expr() -> String {
    "gateway/consumer/**".to_string()
}

fn default_stats_interval() -> u64 {
    5
}

impl GatewayConfig {
    /// Load config from a JSON5 file.
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let json = strip_json5_comments(&content);
        let config: GatewayConfig = serde_json::from_str(&json)?;
        Ok(config)
    }

    /// Load config using the standard priority:
    ///   CLI --config <path>  >  GATEWAY_CONFIG env  >  /etc/zenoh-gateway/config.json5
    ///
    /// If no config file is found, returns a default config (all defaults, connects to localhost).
    pub fn load(cli_config_path: Option<&str>) -> Self {
        let path = cli_config_path
            .map(|s| s.to_string())
            .or_else(|| std::env::var("GATEWAY_CONFIG").ok())
            .unwrap_or_else(|| "/etc/zenoh-gateway/config.json5".to_string());

        match Self::from_file(&path) {
            Ok(config) => {
                println!("Loaded config from: {}", path);
                config
            }
            Err(e) => {
                println!("Config file not found or invalid ({}): {}. Using defaults.", path, e);
                Self::default()
            }
        }
    }

    /// Resolve the final gateway ID.
    /// Priority: GATEWAY_ID env > CLI positional arg > config file id field > default "gw-1"
    pub fn resolve_id(&self, cli_id: Option<&str>) -> String {
        std::env::var("GATEWAY_ID")
            .ok()
            .or_else(|| cli_id.map(|s| s.to_string()))
            .unwrap_or_else(|| self.id.clone())
    }

    /// Convert ZenohEndpoints to a zenoh::Config for session creation.
    pub fn to_zenoh_config(endpoints: &ZenohEndpoints) -> zenoh::Config {
        let mut config = zenoh::Config::default();

        if !endpoints.connect.is_empty() {
            let json = serde_json::json!({
                "endpoints": endpoints.connect
            });
            // zenoh Config supports insert_json5 for nested config keys
            if let Err(e) = config.insert_json5("connect", &json.to_string()) {
                eprintln!("Warning: failed to set connect endpoints: {}", e);
            }
        }

        if !endpoints.listen.is_empty() {
            let json = serde_json::json!({
                "endpoints": endpoints.listen
            });
            if let Err(e) = config.insert_json5("listen", &json.to_string()) {
                eprintln!("Warning: failed to set listen endpoints: {}", e);
            }
        }

        config
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            id: default_id(),
            upstream: ZenohEndpoints::default(),
            downstream: ZenohEndpoints::default(),
            cluster_expr: default_cluster_expr(),
            consumer_liveliness_expr: default_consumer_liveliness_expr(),
            stats_interval_secs: default_stats_interval(),
        }
    }
}

/// Strip JSON5 comments (both // and /* */) from a string,
/// producing valid JSON that serde_json can parse.
/// Handles string literals correctly to avoid stripping inside strings.
fn strip_json5_comments(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Handle string literals — copy verbatim
        if chars[i] == '"' {
            result.push(chars[i]);
            i += 1;
            while i < len {
                result.push(chars[i]);
                if chars[i] == '\\' && i + 1 < len {
                    // Escaped character inside string
                    i += 1;
                    result.push(chars[i]);
                } else if chars[i] == '"' {
                    break;
                }
                i += 1;
            }
            i += 1;
        }
        // Handle single-line comments //
        else if chars[i] == '/' && i + 1 < len && chars[i + 1] == '/' {
            i += 2;
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            // Keep the newline for line number consistency
        }
        // Handle multi-line comments /* */
        else if chars[i] == '/' && i + 1 < len && chars[i + 1] == '*' {
            i += 2;
            while i + 1 < len && !(chars[i] == '*' && chars[i + 1] == '/') {
                i += 1;
            }
            if i + 1 < len {
                i += 2; // Skip */
            }
        }
        else {
            result.push(chars[i]);
            i += 1;
        }
    }

    // Strip trailing commas before } or ] (JSON5 allows them, JSON does not)
    let stripped = strip_trailing_commas(&result);
    stripped
}

/// Remove trailing commas before closing braces/brackets.
/// e.g., {"a": 1,} → {"a": 1}
fn strip_trailing_commas(input: &str) -> String {
    let trimmed = input.trim_end();
    let mut result = String::with_capacity(trimmed.len());
    let chars: Vec<char> = trimmed.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        if chars[i] == ',' {
            // Look ahead past whitespace for } or ]
            let mut j = i + 1;
            while j < len && chars[j].is_whitespace() {
                j += 1;
            }
            if j < len && (chars[j] == '}' || chars[j] == ']') {
                // Skip the trailing comma
                i += 1;
                continue;
            }
        }
        result.push(chars[i]);
        i += 1;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_single_line_comments() {
        let input = r#"{ "id": "gw-a", // comment
"upstream": { "connect": [] } }"#;
        let result = strip_json5_comments(input);
        assert!(!result.contains("//"));
        assert!(result.contains("\"id\": \"gw-a\""));
    }

    #[test]
    fn test_strip_multi_line_comments() {
        let input = r#"{ /* comment */ "id": "gw-a" }"#;
        let result = strip_json5_comments(input);
        assert!(!result.contains("/*"));
        assert!(result.contains("\"id\": \"gw-a\""));
    }

    #[test]
    fn test_strip_trailing_commas() {
        let input = r#"{ "a": 1, "b": 2, }"#;
        let result = strip_trailing_commas(input);
        assert_eq!(result, r#"{ "a": 1, "b": 2 }"#);
    }

    #[test]
    fn test_preserve_string_with_slashes() {
        let input = r#"{ "url": "https://example.com" }"#;
        let result = strip_json5_comments(input);
        assert!(result.contains("https://example.com"));
    }

    #[test]
    fn test_config_default() {
        let config = GatewayConfig::default();
        assert_eq!(config.id, "gw-1");
        assert_eq!(config.cluster_expr, "gateway/cluster/**");
        assert_eq!(config.stats_interval_secs, 5);
    }

    #[test]
    fn test_resolve_id_priority() {
        let config = GatewayConfig {
            id: "from-config".to_string(),
            ..Default::default()
        };

        // Config file id
        assert_eq!(config.resolve_id(None), "from-config");

        // CLI overrides config
        assert_eq!(config.resolve_id(Some("from-cli")), "from-cli");
    }
}
