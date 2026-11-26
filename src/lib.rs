pub mod common;           // privé : interne à la lib
pub mod client;       // public : utilisable de l'extérieur
pub mod server;

pub use client::PSClient;
pub use server::PSServer;