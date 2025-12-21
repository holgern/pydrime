"""Server commands for running storage protocol servers backed by Drime Cloud."""

from pathlib import Path
from typing import Any, Optional

import click

from ..api import DrimeClient
from ..auth import require_api_key
from ..config import config
from ..output import OutputFormatter
from .helpers import _load_htpasswd_for_server


@click.group()
@click.pass_context
def server(ctx: Any) -> None:
    """Start storage servers using Drime Cloud credentials.

    Commands for running various storage protocol servers backed by Drime Cloud.
    Currently supports:
      - restic: REST server for restic backup software
      - s3: S3-compatible server for AWS CLI, rclone, etc.
      - webdav: WebDAV server for mounting as network drive

    Examples:
        pydrime server restic                    # Start restic server on :8000
        pydrime server restic --listen :8001     # Custom port
        pydrime server restic --append-only      # Read-only mode
        pydrime server s3                        # Start S3 server on :9000
        pydrime server s3 --workspace 1593       # Specific workspace
        pydrime server webdav                    # Start WebDAV server on :8080
        pydrime server webdav --readonly         # Read-only WebDAV mount
    """
    pass


@server.command()
@click.option("--listen", default=":8000", help="listen address")
@click.option(
    "--no-auth",
    is_flag=True,
    default=True,
    help="disable .htpasswd authentication (default: disabled)",
)
@click.option(
    "--htpasswd-file", default=None, help="location of .htpasswd file (enables auth)"
)
@click.option("--tls", is_flag=True, help="turn on TLS support")
@click.option("--tls-cert", default=None, help="TLS certificate path")
@click.option("--tls-key", default=None, help="TLS key path")
@click.option("--append-only", is_flag=True, help="enable append only mode")
@click.option(
    "--private-repos", is_flag=True, help="users can only access their private repo"
)
@click.option("--debug", is_flag=True, help="output debug messages")
@click.option(
    "--log",
    default=None,
    help='write HTTP requests in combined log format (use "-" for stdout)',
)
@click.option("--prometheus", is_flag=True, help="enable Prometheus metrics")
@click.option(
    "--prometheus-no-auth",
    is_flag=True,
    help="disable auth for Prometheus /metrics endpoint",
)
@click.option(
    "--no-verify-upload",
    is_flag=True,
    help="do not verify integrity of uploaded data",
)
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses pydrime default if not specified)",
)
@click.pass_context
def restic(
    ctx: Any,
    listen: str,
    no_auth: bool,
    htpasswd_file: Optional[str],
    tls: bool,
    tls_cert: Optional[str],
    tls_key: Optional[str],
    append_only: bool,
    private_repos: bool,
    debug: bool,
    log: Optional[str],
    prometheus: bool,
    prometheus_no_auth: bool,
    no_verify_upload: bool,
    workspace: Optional[int],
) -> None:
    """Start a restic REST server backed by Drime Cloud.

    This command starts a restic-compatible REST server that stores
    backup data in Drime Cloud. It uses your pydrime configuration
    for authentication.

    Authentication:
        By default, authentication is DISABLED for convenience.
        To enable authentication, use --htpasswd-file option.

    Examples:
        pydrime server restic
            Start server on port 8000 without authentication

        pydrime server restic --listen :9000
            Start server on custom port 9000

        pydrime server restic --htpasswd-file /path/to/.htpasswd
            Start server with HTTP basic authentication

        pydrime server restic --append-only
            Start server in append-only mode (no deletes)

        pydrime server restic --workspace 1593
            Use specific workspace instead of default

        pydrime server restic --tls --tls-cert cert.pem --tls-key key.pem
            Start server with TLS/HTTPS support
    """
    out: OutputFormatter = ctx.obj["out"]

    # Check if pyrestserver is installed
    try:
        from pyrestserver.providers.drime import (  # type: ignore[import-not-found]
            DrimeStorageProvider,
        )
        from pyrestserver.server import (  # type: ignore[import-not-found]
            run_rest_server,
        )
    except ImportError:
        out.error("pyrestserver is not installed.")
        out.info("Install with: pip install pydrime[server]")
        out.info("Or: pip install pyrestserver[drime]")
        ctx.exit(1)
        return  # Unreachable but satisfies type checker

    # Get API key from pydrime config
    api_key = require_api_key(ctx, out)

    # Use workspace from pydrime config if not specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    # Initialize Drime client
    client = DrimeClient(api_key=api_key)

    # Create Drime storage provider
    provider = DrimeStorageProvider(
        client=client,
        config={"workspace_id": workspace},
        readonly=append_only,
    )

    # Display startup information (matching pyrestserver style)
    if not out.quiet:
        from rich.console import Console

        console = Console()

        console.print("Storage backend: Drime Cloud")
        console.print(f"Workspace ID: {workspace}")

        if no_auth and not htpasswd_file:
            console.print("Authentication disabled")
        elif htpasswd_file:
            console.print("Authentication enabled")

        if append_only:
            console.print("Append only mode enabled")
        else:
            console.print("Append only mode disabled")

        if private_repos:
            console.print("Private repositories enabled")
            console.print(
                "[yellow]Warning: Private repositories not yet implemented[/yellow]"
            )
        else:
            console.print("Private repositories disabled")

        if prometheus:
            console.print("Prometheus metrics enabled")
            console.print(
                "[yellow]Warning: Prometheus metrics not yet implemented[/yellow]"
            )

        if no_verify_upload:
            console.print("[yellow]Upload verification disabled[/yellow]")
        else:
            console.print("Upload verification enabled")

    # Parse listen address
    if listen.startswith(":"):
        host = "0.0.0.0"
        port = int(listen[1:])
    else:
        if ":" in listen:
            host, port_str = listen.rsplit(":", 1)
            port = int(port_str)
        else:
            host = listen
            port = 8000

    # Setup TLS paths
    ssl_cert_path: Optional[str] = None
    ssl_key_path: Optional[str] = None

    if tls:
        if tls_cert and tls_key:
            ssl_cert_path = tls_cert
            ssl_key_path = tls_key
        else:
            out.error("TLS enabled but --tls-cert and --tls-key not provided")
            ctx.exit(1)

        if not out.quiet:
            from rich.console import Console

            console = Console()
            console.print(
                f"TLS enabled, private key {ssl_key_path}, pubkey {ssl_cert_path}"
            )

    # Setup authentication
    username: Optional[str] = None
    password: Optional[str] = None

    if htpasswd_file and not no_auth:
        # Load htpasswd file
        username, password = _load_htpasswd_for_server(Path(htpasswd_file), out)
        if not username or not password:
            out.warning("Could not load credentials from htpasswd file")
            out.info("Starting server without authentication")

    # Setup logging
    if debug:
        import logging

        from rich.console import Console
        from rich.logging import RichHandler

        logging.basicConfig(
            level=logging.DEBUG,
            format="%(message)s",
            handlers=[
                RichHandler(console=Console(), rich_tracebacks=True, show_time=False)
            ],
        )

    # Start the server
    try:
        run_rest_server(
            provider=provider,
            host=host,
            port=port,
            username=username,
            password=password,
            ssl_cert=ssl_cert_path,
            ssl_key=ssl_key_path,
            no_verify_upload=no_verify_upload,
        )
    except Exception as e:
        out.error(f"Server error: {e}")
        if debug:
            import traceback

            traceback.print_exc()
        ctx.exit(1)


@server.command()
@click.option(
    "--listen",
    default=":9000",
    show_default=True,
    help="Listen address (e.g., :9000, localhost:9000, 0.0.0.0:9000)",
)
@click.option(
    "--access-key-id",
    default="minioadmin",
    show_default=True,
    help="AWS access key ID for authentication",
)
@click.option(
    "--secret-access-key",
    default="minioadmin",
    show_default=True,
    help="AWS secret access key for authentication",
)
@click.option(
    "--region",
    default="us-east-1",
    show_default=True,
    help="AWS region name",
)
@click.option(
    "--no-auth",
    is_flag=True,
    default=True,
    show_default=True,
    help="Disable authentication (allows anonymous access)",
)
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses pydrime default if not specified)",
)
@click.option(
    "--root-folder",
    default=None,
    help="Root folder path in Drime to limit S3 access scope",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging",
)
@click.pass_context
def s3(
    ctx: Any,
    listen: str,
    access_key_id: str,
    secret_access_key: str,
    region: str,
    no_auth: bool,
    workspace: Optional[int],
    root_folder: Optional[str],
    debug: bool,
) -> None:
    """Start an S3-compatible server backed by Drime Cloud.

    This command starts a local S3-compatible server that uses your Drime Cloud
    storage as the backend. You can then use any S3-compatible client (aws-cli,
    rclone, s3cmd, etc.) to access your Drime files.

    Authentication:
        By default, authentication is DISABLED for convenience.
        To enable authentication, remove --no-auth and set custom credentials.

    Examples:
        pydrime server s3
            Start server on port 9000 without authentication

        pydrime server s3 --listen :9001
            Start server on custom port 9001

        pydrime server s3 --access-key-id mykey --secret-access-key mysecret
            Start with custom credentials (auth enabled)

        pydrime server s3 --root-folder /backup
            Limit access to specific folder

        pydrime server s3 --workspace 123
            Use specific workspace instead of default
    """
    out: OutputFormatter = ctx.obj["out"]

    # Check if pys3local is installed
    try:
        import uvicorn  # type: ignore[import-not-found]
        from pys3local.providers.drime import (  # type: ignore[import-not-found]
            DrimeStorageProvider,
        )
        from pys3local.server import create_s3_app  # type: ignore[import-not-found]
    except ImportError:
        out.error("pys3local is not installed.")
        out.info("Install with: pip install pydrime[server]")
        out.info("Or: pip install pys3local[drime]")
        ctx.exit(1)
        return  # Unreachable but satisfies type checker

    # Get API key from pydrime config
    api_key = require_api_key(ctx, out)

    # Use workspace from pydrime config if not specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    # Initialize Drime client
    client = DrimeClient(api_key=api_key)

    # Parse listen address
    if listen.startswith(":"):
        host = "0.0.0.0"
        port = int(listen[1:])
    else:
        if ":" in listen:
            host, port_str = listen.rsplit(":", 1)
            port = int(port_str)
        else:
            host = listen
            port = 9000

    # Create Drime storage provider
    provider = DrimeStorageProvider(
        client=client, workspace_id=workspace, root_folder=root_folder, readonly=False
    )

    # Display startup information
    if not out.quiet:
        from rich.console import Console

        console = Console()

        console.print("Storage backend: Drime Cloud")
        console.print(f"Workspace ID: {workspace}")

        if no_auth:
            console.print("[yellow]Authentication disabled[/yellow]")
        else:
            console.print("[green]Authentication enabled[/green]")
            console.print(f"Access Key ID: [cyan]{access_key_id}[/cyan]")
            console.print(f"Secret Access Key: [cyan]{secret_access_key}[/cyan]")
            console.print(f"Region: [cyan]{region}[/cyan]")

        if root_folder:
            console.print(f"Root folder: {root_folder}")

        console.print(f"\n[green]Starting S3 server at http://{host}:{port}/[/green]\n")

        # Show connection examples
        console.print("[bold]rclone configuration:[/bold]")
        console.print("[dim]Add this to ~/.config/rclone/rclone.conf:[/dim]")
        console.print()
        console.print("[pydrime-s3]")
        console.print("type = s3")
        console.print("provider = Other")

        if not no_auth:
            console.print(f"access_key_id = {access_key_id}")
            console.print(f"secret_access_key = {secret_access_key}")
        else:
            console.print("access_key_id = test")
            console.print("secret_access_key = test")

        endpoint_display = f"http://{host if host != '0.0.0.0' else 'localhost'}:{port}"
        console.print(f"endpoint = {endpoint_display}")
        console.print(f"region = {region}")
        console.print()

        console.print("[dim]Press Ctrl+C to stop the server[/dim]\n")

    # Start the server
    try:
        # Create S3 app
        app = create_s3_app(
            provider=provider,
            access_key=access_key_id,
            secret_key=secret_access_key,
            region=region,
            no_auth=no_auth,
        )

        # Run server with uvicorn
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level="error" if not debug else "info",
            server_header=False,
            timeout_keep_alive=75,
            access_log=debug,
        )
    except KeyboardInterrupt:
        if not out.quiet:
            from rich.console import Console

            console = Console()
            console.print("\n[yellow]Server stopped[/yellow]")
        ctx.exit(0)
    except Exception as e:
        out.error(f"Server error: {e}")
        if debug:
            import traceback

            traceback.print_exc()
        ctx.exit(1)


@server.command()
@click.option(
    "--host",
    default="0.0.0.0",
    show_default=True,
    help="Host address to bind to",
)
@click.option(
    "--port",
    type=int,
    default=8080,
    show_default=True,
    help="Port number to listen on",
)
@click.option(
    "--username",
    default=None,
    help="WebDAV username for authentication (omit for anonymous access)",
)
@click.option(
    "--password",
    default=None,
    help="WebDAV password for authentication",
)
@click.option(
    "--no-auth",
    is_flag=True,
    default=True,
    show_default=True,
    help="Disable authentication (allows anonymous access)",
)
@click.option(
    "--readonly",
    is_flag=True,
    default=False,
    help="Enable read-only mode (no writes allowed)",
)
@click.option(
    "--workspace",
    "-w",
    type=int,
    default=None,
    help="Workspace ID (uses pydrime default if not specified)",
)
@click.option(
    "--cache-ttl",
    type=float,
    default=300.0,
    show_default=True,
    help="Cache TTL in seconds for Drime backend",
)
@click.option(
    "--max-file-size",
    type=int,
    default=5368709120,
    help="Maximum file size in bytes (default: 5GB)",
)
@click.option(
    "--ssl-cert",
    type=click.Path(exists=True),
    help="Path to SSL certificate file (for HTTPS)",
)
@click.option(
    "--ssl-key",
    type=click.Path(exists=True),
    help="Path to SSL private key file (for HTTPS)",
)
@click.option(
    "--verbose",
    "-v",
    count=True,
    help="Increase verbosity (-v, -vv, -vvv for more detail)",
)
@click.pass_context
def webdav(
    ctx: Any,
    host: str,
    port: int,
    username: Optional[str],
    password: Optional[str],
    no_auth: bool,
    readonly: bool,
    workspace: Optional[int],
    cache_ttl: float,
    max_file_size: int,
    ssl_cert: Optional[str],
    ssl_key: Optional[str],
    verbose: int,
) -> None:
    """Start a WebDAV server backed by Drime Cloud.

    This command starts a local WebDAV server that uses your Drime Cloud
    storage as the backend. You can then mount it as a network drive on
    Windows, macOS, or Linux.

    Authentication:
        By default, authentication is DISABLED for convenience.
        To enable authentication, provide both --username and --password.

    Examples:
        pydrime server webdav
            Start server on 0.0.0.0:8080 without authentication

        pydrime server webdav --port 9090
            Start server on custom port 9090

        pydrime server webdav --username user --password pass
            Start with authentication enabled

        pydrime server webdav --readonly
            Start in read-only mode (no modifications allowed)

        pydrime server webdav --workspace 123
            Use specific workspace instead of default

        pydrime server webdav --ssl-cert cert.pem --ssl-key key.pem
            Start with HTTPS support
    """
    out: OutputFormatter = ctx.obj["out"]

    # Check if pywebdavserver is installed
    try:
        from pywebdavserver.providers.drime import (  # type: ignore[import-not-found]
            DrimeDAVProvider,
        )
        from pywebdavserver.server import (  # type: ignore[import-not-found]
            run_webdav_server,
        )
    except ImportError:
        out.error("pywebdavserver is not installed.")
        out.info("Install with: pip install pydrime[server]")
        out.info("Or: pip install pywebdavserver[drime]")
        ctx.exit(1)
        return  # Unreachable but satisfies type checker

    # Get API key from pydrime config
    api_key = require_api_key(ctx, out)

    # Use workspace from pydrime config if not specified
    if workspace is None:
        workspace = config.get_default_workspace() or 0

    # Initialize Drime client
    client = DrimeClient(api_key=api_key)

    # Handle authentication
    if no_auth:
        username = None
        password = None
    elif username and not password:
        out.warning("Username provided but no password. Using anonymous access.")
        username = None
    elif not username and password:
        out.warning("Password provided but no username. Using anonymous access.")
        password = None

    # Create Drime DAV provider
    try:
        provider = DrimeDAVProvider(
            client=client,
            workspace_id=workspace,
            readonly=readonly,
            cache_ttl=cache_ttl,
            max_file_size=max_file_size,
        )
    except Exception as e:
        out.error(f"Failed to initialize Drime provider: {e}")
        ctx.exit(1)
        return  # Unreachable but satisfies type checker

    # Display startup information
    if not out.quiet:
        from rich.console import Console

        console = Console()

        console.print("\n[bold green]Starting WebDAV Server[/bold green]")
        console.print("  Backend: [cyan]Drime Cloud[/cyan]")
        console.print(f"  Workspace ID: [cyan]{workspace}[/cyan]")
        console.print(f"  Address: [cyan]{host}:{port}[/cyan]")
        console.print(
            f"  Mode: [cyan]{'Read-only' if readonly else 'Read-write'}[/cyan]"
        )

        if no_auth or not username:
            console.print("  Auth: [yellow]Disabled (anonymous access)[/yellow]")
        else:
            console.print(f"  Auth: [cyan]Enabled (user: {username})[/cyan]")

        if ssl_cert and ssl_key:
            console.print("  SSL: [cyan]Enabled[/cyan]")
            protocol = "https"
        else:
            protocol = "http"

        console.print()
        console.print("[bold]Mount WebDAV share:[/bold]")

        # Connection URL
        url = f"{protocol}://{host if host != '0.0.0.0' else 'localhost'}:{port}/"

        console.print(f"  URL: [cyan]{url}[/cyan]")
        console.print()
        console.print("[bold]Platform-specific instructions:[/bold]")
        console.print("  Windows:  Map network drive in File Explorer")
        console.print("  macOS:    Finder → Go → Connect to Server")
        console.print(f"  Linux:    nautilus {url}")
        console.print()
        console.print("[yellow]Press Ctrl+C to stop the server[/yellow]")
        console.print()

    # Start the server
    try:
        server_name = f"PyWebDAV Server (Drime: workspace {workspace})"

        run_webdav_server(
            provider=provider,
            host=host,
            port=port,
            username=username,
            password=password,
            verbose=verbose + 1,  # Adjust verbosity for server
            ssl_cert=ssl_cert,
            ssl_key=ssl_key,
            server_name=server_name,
        )
    except Exception as e:
        out.error(f"Server error: {e}")
        if verbose > 0:
            import traceback

            traceback.print_exc()
        ctx.exit(1)
