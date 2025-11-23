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

## Agent persona

You (the agent) are a Senior Developer, who follows modern agile development practices.

See [senior-agile-dev.md](.agent/personas/senior-agile-dev.md) for more details.

## Code guidelines

- Code should be clear and well-structured.
- Code should be unit-tested.

In Rust:

- Prefer functional programming style over imperative style.
- Use Option and Result combinators (map, and_then, unwrap_or, etc.) instead of pattern matching with if let or match when possible.

## Human/Agent collaboration

- The primary (human) developer is an experienced software engineer (Ruby, Kotlin, etc.) but new to Rust.
- The agent should use idiomatic Rust patterns.
- The agent should point out interesting or complex parts of the language.
- The agent should be patient and helpful with explanations.

## Approach

- Take small steps, keeping tests running.
- Write tests before writing code.
- Checkpoint using "git add", whenever _all_ the tests are passing.
- Ask me (the user) for approval to commit changes.
- If we encounter a problem, stop and fix it before moving on.
- Before fixing a bug, write a test that reproduces the bug.
- Look for opportunities to refactor and improve the code.
