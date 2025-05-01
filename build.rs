fn main() -> Result<(), String> {
    use std::io::Write;

    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto/datafusion_common.proto");
    println!("cargo:rerun-if-changed=proto/extension.proto");

    tonic_build::configure()
        .extern_path(".datafusion_common", "::datafusion_proto_common")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/extension.proto"], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    //
    // we copy generated data to src folder
    //
    let generated_source_path = out.join("extension.ballista.rs");
    let code = std::fs::read_to_string(generated_source_path).unwrap();

    let path = "src/codec/messages.rs";
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        .unwrap();

    file.write_all(code.as_str().as_ref()).unwrap();

    Ok(())
}
