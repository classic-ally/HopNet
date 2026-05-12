#[cfg(target_os = "macos")]
use super::keychain::{
    FileProviderConfig, KeychainEnvironment, load_config, remove_config, store_config,
};
use uuid::Uuid;

#[cfg(target_os = "macos")]
#[tokio::test]
async fn test_keychain_setup() {
    // Clean up any existing test data first
    let _ = remove_config(KeychainEnvironment::Test);

    // Generate random test configuration to avoid false positives from stale data
    let random_api_key = format!("test-api-key-{}", Uuid::new_v4());
    let random_port = 30000 + (rand::random::<u16>() % 10000); // Random port 30000-39999
    let test_base_url = format!("http://localhost:{}", random_port);

    let test_config = FileProviderConfig::new(random_api_key.clone(), test_base_url.clone());

    // Store test configuration
    store_config(&test_config, KeychainEnvironment::Test)
        .expect("Failed to store test config in keychain");

    // Load it back
    let loaded_config =
        load_config(KeychainEnvironment::Test).expect("Failed to load test config from keychain");

    // Verify it matches our randomly generated values (not any stale data)
    assert_eq!(
        loaded_config.api_key, random_api_key,
        "API key should match the randomly generated value"
    );
    assert_eq!(
        loaded_config.base_url, test_base_url,
        "Base URL should match the randomly generated value"
    );

    println!("✅ Keychain test setup working correctly with random values");
    println!("   API Key: {}", loaded_config.api_key);
    println!("   Base URL: {}", loaded_config.base_url);

    // Clean up test data
    let _ = remove_config(KeychainEnvironment::Test);

    // Verify cleanup worked by trying to load again (should fail)
    match load_config(KeychainEnvironment::Test) {
        Ok(_) => panic!("Expected keychain cleanup to remove test config, but it still exists"),
        Err(_) => println!("✅ Keychain cleanup successful - config no longer loadable"),
    }
}
