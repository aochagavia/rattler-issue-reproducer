use std::path::{Path};
use anyhow::Context;
use bytes::Bytes;
use rattler::install::{InstallDriver, InstallOptions};
use rattler_conda_types::package::ArchiveType;

static FORTRAN_DIR: &str = "target/test-tmp/fortran";
static TARGET_PREFIX_DIR: &str = "/foo/bar/baz";
static REAL_PREFIX_DIR: &str = "target/test-tmp/prefix";

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Fetch
    let client = reqwest::Client::new();
    let (archive_type, bytes) = fetch_package(&client, "https://conda.anaconda.org/conda-forge/linux-64/libgfortran-3.0.0-1.tar.bz2").await?;

    // Repeatedly extract + link until we hit the bug
    for i in 0..100 {
        println!("Round {}", i + 1);
        println!("=======");
        run(archive_type, bytes.clone()).await?;
        println!();
    }

    Ok(())
}

async fn run(archive_type: ArchiveType, bytes: Bytes) -> anyhow::Result<()> {
    // Make sure the relevant dirs exist and are empty
    std::fs::remove_dir_all("target/test-tmp").ok();
    std::fs::create_dir_all("target/test-tmp")?;

    // Extract
    extract_package(archive_type, bytes)?;

    // Link
    let install_options = InstallOptions {
        target_prefix: Some(TARGET_PREFIX_DIR.into()),
        python_info: None,
        ..InstallOptions::default()
    };

    rattler::install::link_package(
        FORTRAN_DIR.as_ref(),
        REAL_PREFIX_DIR.as_ref(),
        &InstallDriver::default(),
        install_options,
    ).await?;

    Ok(())
}

async fn fetch_package(client: &reqwest::Client, package_url: &str) -> anyhow::Result<(ArchiveType, Bytes)> {
    let archive_type = ArchiveType::try_from(Path::new(package_url))
        .context("unsupported archive type")?;

    let response = loop {
        let result = client.get(package_url).send().await;
        match result {
            Ok(response) => break response,
            Err(e) => return Err(e).context("unable to fetch package"),
        }
    };

    let body = response.bytes().await.context("unable to stream response body")?;

    Ok((archive_type, body))
}

fn extract_package(archive_type: ArchiveType, bytes: Bytes) -> anyhow::Result<()> {
    let package_path = Path::new(FORTRAN_DIR);
    let result = match archive_type {
        ArchiveType::TarBz2 => {
            rattler_package_streaming::read::extract_tar_bz2(bytes.as_ref(), &package_path)
        }
        ArchiveType::Conda => {
            rattler_package_streaming::read::extract_conda(bytes.as_ref(), &package_path)
        }
    };

    result?;
    Ok(())
}
