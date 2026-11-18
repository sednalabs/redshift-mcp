//! Native stdio entry point. Stdout is reserved for MCP except explicit CLI commands.

use mcp_toolkit::{
    client_config::{render_executable_client_config, ClientConfigFormat},
    doctor::inspect_project,
    server::{
        stdio::StdioServerBuilder,
        tools::{
            render_tool_surface_command, render_tool_surface_help, tool_surface_command_from_env,
            ToolSurfaceCommand,
        },
    },
};
use redshift_mcp::{Config, RedshiftServer};

type MainResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
const BINARY_NAME: &str = "redshift-mcp";

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Only our allowlisted configuration errors reach this path; transport
        // failures are replaced below to avoid printing protocol payloads.
        eprintln!("{error}");
        std::process::exit(2);
    }
}

async fn run() -> MainResult {
    let command = tool_surface_command_from_env()
        .map_err(|_| std::io::Error::other("unknown argument; use --help"))?;
    match command {
        ToolSurfaceCommand::Help => print!("{}", render_tool_surface_help(BINARY_NAME)),
        ToolSurfaceCommand::PrintClientConfig => {
            // Never copy the caller's AWS environment into output.
            print!(
                "{}",
                render_executable_client_config(
                    BINARY_NAME,
                    &std::env::current_exe()?,
                    ClientConfigFormat::KiroJson,
                    &[("REDSHIFT_TOOL_PROFILE", "analytics")]
                )?
            );
        }
        ToolSurfaceCommand::Doctor => {
            let report = inspect_project(std::env::current_dir()?);
            print!("{}", report.render());
            if !report.ready() {
                return Err(std::io::Error::other("project checks failed").into());
            }
        }
        ToolSurfaceCommand::Serve => {
            let server = RedshiftServer::new(Config::from_env()?)?;
            StdioServerBuilder::new()
                .serve(server)
                .await
                .map_err(|_| std::io::Error::other("MCP transport ended with an error"))?;
        }
        command => {
            let server = RedshiftServer::new(Config::from_env()?)?;
            let tools = server.active_tools()?;
            if let Some(output) = render_tool_surface_command(command, &tools)? {
                print!("{output}");
            }
        }
    }
    Ok(())
}
