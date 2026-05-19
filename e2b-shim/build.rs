fn main() -> std::io::Result<()> {
    prost_build::compile_protos(
        &[
            "../proto/envd/process.proto",
            "../proto/envd/filesystem.proto",
        ],
        &["../proto/envd"],
    )?;
    Ok(())
}
