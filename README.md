# TCP Connection Example Between Two Services

This is a simple example demonstrating how two services can communicate with each other using a custom TCP connection.

## Purpose

The goal of this project is to illustrate how a **client** and a **server** can communicate over raw TCP without relying on high-level networking libraries.

## Structure

The project contains two binaries:

- `server`: listens for incoming TCP connections.
- `client`: initiates a TCP connection to the server and sends data.

## How to Run

Make sure you have Rust installed. Then, you can build and run the binaries as follows:

### Run the server

```bash
cargo run --bin server
```

### Run the client

```bash
cargo run --bin client
```