# Learning Rust with `bergr`

This document maps Rust concepts to specific examples within the `bergr` codebase. It is designed to help you understand the language by looking at code you are already working with.

## 1. Project Structure & Modules ✅

Rust projects are organized into "crates" (packages) and "modules".

-   **Binary vs Library**:
    -   `src/main.rs` ([link](file:///src/main.rs)): The entry point for the executable binary.
    -   `src/lib.rs` ([link](file:///src/lib.rs)): Defines the library structure. It's common to put most logic in a library and keep `main.rs` thin.

-   **Modules**:
    -   In `src/lib.rs`, `pub mod cli;` tells Rust to look for a file named `cli.rs` and include it as a module.
    -   `use bergr::cli::Cli;` in `main.rs` brings the `Cli` struct into scope.

## 2. Data Modeling (Structs & Enums) ✅

Rust's type system is centered around Structs (product types) and Enums (sum types).

-   **Structs**:
    -   See `pub struct Cli` in `src/cli.rs` ([link](file:///src/cli.rs)). It defines a custom data type with named fields (`command`, `debug`).
    -   `#[derive(Parser, Debug)]`: These are "attributes". They ask the compiler to automatically implement specific "Traits" (interfaces) for this struct. `Parser` comes from `clap` (CLI argument parsing), and `Debug` allows formatting the struct for printing (e.g., `{:?}`).

-   **Enums**:
    -   See `pub enum Commands` in `src/cli.rs`. Unlike Enums in other languages, Rust enums can hold data.
    -   `At { location: String, command: AtCommands }`: This variant holds two pieces of data.

## 3. Control Flow & Pattern Matching ✅

Rust uses `match` expressions to handle Enums safely. You must handle every possible variant.

-   **Matching**:
    -   See `match cli.command` in `src/main.rs` ([link](file:///src/main.rs)).
    -   `Commands::At { location, command } => { ... }`: This "destructures" the enum variant, extracting `location` and `command` variables to use inside the block.

## 4. Asynchronous Programming ✅

Rust's standard library is synchronous. We use `tokio` for async capabilities.

-   **Async/Await**:
    -   `async fn main()` in `src/main.rs`: Marks the function as asynchronous.
    -   `.await`: In `src/main.rs`, `handle_at_command(...).await?` pauses execution of the function until the future completes.
-   **Runtime**:
    -   `#[tokio::main]`: This macro sets up the hidden boilerplate code to start the `tokio` runtime and execute the `main` function.

## 5. Error Handling ✅

Rust doesn't use exceptions. It returns a `Result` type that is either `Ok(value)` or `Err(error)`.

-   **The `Result` Type**:
    -   `fn main() -> Result<()>`: The program returns a Result.
-   **The `?` Operator**:
    -   In `src/commands.rs` ([link](file:///src/commands.rs)): `let metadata = fetch_metadata(client, location).await?;`
    -   If `fetch_metadata` returns `Err`, the `?` operator immediately returns that error from the current function.
    -   If it returns `Ok(val)`, `val` is assigned to `metadata`.
-   **Anyhow**:
    -   We use the `anyhow` crate for flexible error handling. `.context("...")` wraps an error with more details.

## 6. Ownership & Borrowing ✅

This is Rust's unique memory management system.

-   **Ownership**: Every value has a single owner. When the owner goes out of scope, the value is dropped.
-   **Borrowing**:
    -   Look at `handle_at_command` in `src/commands.rs`.
    -   `client: &aws_sdk_s3::Client`: The `&` means we are "borrowing" the client. The caller (`main.rs`) still owns it.
    -   `location: &str`: We borrow a string slice.
    -   If we used `client: aws_sdk_s3::Client` (no `&`), the function would take ownership, and `main.rs` couldn't use it afterwards.

## 7. Iterators & Closures ✅

Rust leans heavily on functional programming patterns.

-   **Iterators**:
    -   In `src/commands.rs`: `metadata.schemas_iter().collect()`
    -   `metadata.snapshots().find(|s| ...)`: lazily iterates and searches.
-   **Closures**:
    -   `|s| s.snapshot_id() == id`: This is a closure (lambda/anonymous function).
    -   See `iterate_files` in `src/commands.rs`. It takes a generic parameter `F` where `F: FnMut(...)`. This allows passing a closure that is called for every file found.

## 8. Testing ✅

-   **Inline Tests**:
    -   See `mod tests` at the bottom of `src/commands.rs`. Rust tests often live in the same file as the code.
    -   `#[tokio::test]`: runs the test in an async context.
    -   `#[cfg(test)]`: Ensures this code is only compiled when running `cargo test`.
