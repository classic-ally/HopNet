plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
    alias(libs.plugins.kotlin.serialization)
}

// RFC-023: the app's client identity is the workspace CalVer, read from the
// repo-root Cargo.toml at configure time. A malformed token fails the build —
// the same boot-time invariant the node enforces on its own version.
val workspaceCalVer: String = rootProject.file("../../Cargo.toml").readLines().let { lines ->
    val start = lines.indexOfFirst { it.trim() == "[workspace.package]" }
    require(start >= 0) { "no [workspace.package] section in workspace Cargo.toml" }
    lines.drop(start + 1)
        .takeWhile { !it.trim().startsWith("[") }
        .firstNotNullOfOrNull {
            Regex("""^version\s*=\s*"([^"]+)"""").find(it.trim())?.groupValues?.get(1)
        }
        ?: error("no version in [workspace.package] of workspace Cargo.toml")
}
val clientVersionCode: Int = Regex("""^(\d{4})\.(\d{1,2})\.(\d{1,2})$""")
    .find(workspaceCalVer)?.let { m ->
        val (year, month, counter) = m.destructured
        require(month.toInt() in 1..12 && counter.toInt() <= 99) {
            "workspace version '$workspaceCalVer' is not CalVer YYYY.M.N (RFC-023)"
        }
        year.toInt() * 10_000 + month.toInt() * 100 + counter.toInt()
    } ?: error("workspace version '$workspaceCalVer' is not CalVer YYYY.M.N (RFC-023)")

android {
    namespace = "app.hopnet.drive"
    compileSdk {
        version = release(36)
    }
    // Pin to the build-tools present in the nix-provisioned SDK (the store
    // is read-only, so AGP must not try to install its own default).
    buildToolsVersion = "36.0.0"

    defaultConfig {
        applicationId = "app.hopnet.drive"
        minSdk = 31
        targetSdk = 36
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

        buildConfigField("int", "HOPNET_CLIENT_VERSION_CODE", "$clientVersionCode")
        buildConfigField("String", "HOPNET_CLIENT_VERSION_NAME", "\"$workspaceCalVer\"")
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
    buildFeatures {
        compose = true
        buildConfig = true
    }
    testOptions {
        unitTests {
            // ApiClient references android.os.CancellationSignal /
            // android.util.Log types on the JVM; with null signals no
            // Android method actually runs.
            isReturnDefaultValues = true
        }
    }
}

dependencies {
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines.android)
    implementation(libs.okhttp)
    // QR pairing without Google Play Services: ZXing core (pure Java)
    // decodes frames from CameraX (Jetpack) — no ML Kit.
    implementation(libs.zxing.core)
    implementation(libs.androidx.camera.core)
    implementation(libs.androidx.camera.camera2)
    implementation(libs.androidx.camera.lifecycle)
    implementation(libs.androidx.camera.view)
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.activity.compose)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.compose.ui)
    implementation(libs.androidx.compose.ui.graphics)
    implementation(libs.androidx.compose.ui.tooling.preview)
    implementation(libs.androidx.compose.material3)
    testImplementation(libs.junit)
    // TLS-capable fake node for JVM transport tests: the SPKI pin is
    // computed from the HeldCertificate, exercising the real pinned client.
    testImplementation(libs.mockwebserver)
    testImplementation(libs.okhttp.tls)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.test.rules)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.compose.ui.test.junit4)
    debugImplementation(libs.androidx.compose.ui.tooling)
    debugImplementation(libs.androidx.compose.ui.test.manifest)
}