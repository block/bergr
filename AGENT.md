# AGENT.md

## Project Intent

This project, "bergr", is a utility for inspecting Apache Iceberg tables.
It is built by "Amp" (the AI agent) and "mikewilliams" (the user) working together.

## Goals

1. **Build a useful tool**: Create a CLI and RESTful API for exploring Iceberg namespaces, tables, metadata, schemas, and files.
2. **Learn Rust**: The user wants to learn Rust through this project. The AI agent should:
   - Explain Rust concepts when introducing new code.
   - Use idiomatic Rust patterns.
   - Point out interesting or complex parts of the language.
   - Be patient and helpful with explanations.

## Architecture

- **Language**: Rust
- **Core Libraries**:
  - `iceberg`: Official Apache Iceberg Rust implementation.
  - `tokio`: Asynchronous runtime.
  - `axum`: Web framework for the REST API.
  - `clap`: Command Line Interface parser.
  - `anyhow`: Error handling.

## Features

- **CLI Mode**: Commands to inspect Iceberg catalogs and tables from the terminal.
- **Server Mode**: A RESTful API to serve Iceberg metadata over HTTP.

## Development workflow

- The user is an experienced software engineer (Ruby, Kotlin, etc.) but new to Rust.
- Code should be clear and well-structured.
- Comments should be used to explain "why" and specific Rust mechanisms.
