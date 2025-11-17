use std::{env, path::PathBuf};

// We will build the `lighter-signer` bindings here
// instead of mapping them manually, since we have the header files from v0.1.3
fn main() {
    let dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    // header file
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    let header = format!("{dir}/libs/linux/amd64/liblighter-signer.h");
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    let header = format!("{dir}/libs/linux/arm64/liblighter-signer.h");
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    let header = format!("{dir}/libs/darwin/arm64/liblighter-signer.h");
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    let header = format!("{dir}/libs/windows/amd64/liblighter-signer.h");

    // tell the linker where to look for for the lib
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        println!("cargo:rustc-link-search=native={dir}/libs/linux/amd64");
        println!("cargo:rustc-link-lib=dylib=lighter-signer");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}/libs/linux/amd64");
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        println!("cargo:rustc-link-search=native={dir}/libs/linux/arm64");
        println!("cargo:rustc-link-lib=dylib=lighter-signer");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}/libs/linux/arm64");
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        println!("cargo:rustc-link-search=native={dir}/libs/darwin/arm64");
        println!("cargo:rustc-link-lib=dylib=lighter-signer");
        println!("cargo:rustc-link-arg=-Wl,-rpath,{dir}/libs/darwin/arm64");
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        println!("cargo:rustc-link-search=native={dir}/libs/windows/amd64");
        println!("cargo:rustc-link-lib=dylib=lighter-signer");
    }

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindgen::Builder::default()
        .header(header)
        .generate_comments(true)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!")
}
