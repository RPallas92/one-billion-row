## Create test data
Generate random file first:

```
cargo run --bin create_data_file
```

You can edit the numnber of rows to generate by editing `scripts/create_data_file.rs`.

## Run the program

Compile a release version and execute it:

```
cargo build --release && ./target/release/one-billion-row  
```