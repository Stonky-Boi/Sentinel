import os
import sys
import time
import json
import signal
import argparse
import subprocess

from core.kafka_client import consume_raw_logs
from core.logger import SENTINEL_HOME
from core.config import Config

# Rich library imports for beautiful terminal UI
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

PID_FILE = SENTINEL_HOME / "sentinel.pid"
REPORT_DIR = SENTINEL_HOME / "reports"

def start_daemon() -> None:
    """Spawns the Sentinel worker pipeline as a detached background process."""
    if PID_FILE.exists():
        print("Sentinel is already running. (PID file exists)")
        sys.exit(1)
    
    print("Starting Sentinel in the background...")
    process = subprocess.Popen(
        [sys.executable, __file__, "run-worker"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )
    
    with open(PID_FILE, "w") as f:
        f.write(str(process.pid))
    
    print(f"Sentinel started successfully. (PID: {process.pid})")

def stop_daemon() -> None:
    """Reads the PID file and gracefully terminates the background worker."""
    if not PID_FILE.exists():
        print("Sentinel is not currently running.")
        sys.exit(0)
        
    with open(PID_FILE, "r") as f:
        pid = int(f.read().strip())
        
    print(f"Stopping Sentinel (PID: {pid})...")
    try:
        os.kill(pid, signal.SIGTERM)
        print("Sentinel stopped.")
    except ProcessLookupError:
        print("Process was not running. Cleaning up stale PID file.")
    
    PID_FILE.unlink(missing_ok=True)

def status_daemon() -> None:
    """Checks if the background process is currently active."""
    if not PID_FILE.exists():
        print("Sentinel is STOPPED.")
        return
        
    with open(PID_FILE, "r") as f:
        pid = int(f.read().strip())
        
    try:
        os.kill(pid, 0)
        print(f"Sentinel is RUNNING (PID: {pid}).")
    except ProcessLookupError:
        print("Sentinel is STOPPED (Stale PID file found and cleaned).")
        PID_FILE.unlink(missing_ok=True)


def monitor_reports() -> None:
    """Continuously monitors the reports directory and displays new incidents in real-time."""
    console = Console()
    console.print("[bold green]Sentinel Monitor Active. Waiting for new incidents...[/bold green] (Press Ctrl+C to exit)\n")
    
    # Record the files that already exist so we don't print old reports
    seen_files = set(f.name for f in REPORT_DIR.glob("*.json"))
    
    try:
        while True:
            current_files = set(f.name for f in REPORT_DIR.glob("*.json"))
            new_files = current_files - seen_files
            
            for new_file in sorted(new_files):
                # The filename is the Report ID (e.g., INC-123.json -> INC-123)
                report_id = new_file.replace(".json", "")
                view_report(report_id)
                seen_files.add(new_file)
                
            time.sleep(1) # Poll every second
    except KeyboardInterrupt:
        console.print("\n[bold red]Monitor stopped.[/bold red]")

def run_worker() -> None:
    """The actual foreground loop, hidden from the user by the start_daemon command."""
    target_topic = Config["kafka"]["topic_raw"]
    consume_raw_logs(topic=target_topic, group_id=Config["kafka"]["consumer_group"])

def list_reports() -> None:
    """Displays a formatted table of all generated incident reports."""
    console = Console()
    files = list(REPORT_DIR.glob("*.json"))
    
    if not files:
        console.print("[bold yellow]No incident reports found.[/bold yellow]")
        return

    # Create a Rich Table
    table = Table(title="Sentinel Incident Reports", box=box.ROUNDED)
    table.add_column("Timestamp", style="cyan", no_wrap=True)
    table.add_column("Report ID", style="magenta")
    table.add_column("Severity", justify="center")
    table.add_column("Title", style="white")

    # Load data and sort newest first
    data = []
    for f in files:
        try:
            with open(f, 'r') as file:
                data.append(json.load(file))
        except Exception:
            continue
            
    data.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

    # Populate the table
    for d in data[:20]:  # Show top 20 recent alerts
        sev = d.get("severity_level", "UNKNOWN").upper()
        # Color code the severities
        color = "red" if sev == "CRITICAL" else "yellow" if sev == "HIGH" else "blue" if sev == "MEDIUM" else "green"
        sev_styled = f"[bold {color}]{sev}[/bold {color}]"
        
        # Format timestamp to be cleaner
        clean_time = d.get("timestamp", "").replace("T", " ")[:19]
        
        table.add_row(clean_time, d.get("report_id", ""), sev_styled, d.get("incident_title", ""))
        
    console.print(table)
    console.print("\n[dim]Use 'python main.py view <Report_ID>' to read full details.[/dim]")

def view_report(report_id: str) -> None:
    """Renders a specific incident report in a beautiful terminal panel."""
    console = Console()
    
    # Allow partial matching (e.g., just passing the last 6 characters)
    files = list(REPORT_DIR.glob(f"*{report_id}*.json"))
    if not files:
        console.print(f"[bold red]Report matching '{report_id}' not found.[/bold red]")
        return
        
    with open(files[0], 'r') as f:
        data = json.load(f)

    sev = data.get("severity_level", "UNKNOWN").upper()
    color = "red" if sev == "CRITICAL" else "yellow" if sev == "HIGH" else "blue" if sev == "MEDIUM" else "green"

    # Format the Action Items into a bulleted list
    actions = "\n".join([f"[bold {color}]•[/bold {color}] {a}" for a in data.get("recommended_actions", [])])

    content = (
        f"[bold]Target IP:[/bold] {data.get('target_ip')}\n"
        f"[bold]Source IP:[/bold] {data.get('source_ip')}\n\n"
        f"[bold underline]Executive Summary:[/bold underline]\n{data.get('executive_summary')}\n\n"
        f"[bold underline]Recommended Actions:[/bold underline]\n{actions}"
    )

    # Render as a bordered panel
    panel = Panel(
        content,
        title=f"[bold {color}]{data.get('incident_title')} ({sev})[/bold {color}]",
        subtitle=f"[dim]ID: {data.get('report_id')} | Time: {data.get('timestamp')}[/dim]",
        border_style=color,
        box=box.HEAVY
    )
    console.print(panel)

def main() -> None:
    parser = argparse.ArgumentParser(description="Sentinel Agent CLI")
    parser.add_argument(
        "command", 
        choices=["start", "stop", "status", "run-worker", "list", "view", "monitor"], 
        help="Control the Sentinel daemon or view reports."
    )
    parser.add_argument(
        "report_id", 
        nargs="?", 
        help="The ID of the report to view (Required for 'view' command)."
    )
    
    args = parser.parse_args()
    SENTINEL_HOME.mkdir(parents=True, exist_ok=True)
    
    if args.command == "start":
        start_daemon()
    elif args.command == "stop":
        stop_daemon()
    elif args.command == "status":
        status_daemon()
    elif args.command == "run-worker":
        run_worker()
    elif args.command == "list":
        list_reports()
    elif args.command == "monitor":
        monitor_reports()
    elif args.command == "view":
        if not args.report_id:
            print("Error: You must provide a Report ID to view. (e.g., 'python main.py view INC-123')")
            sys.exit(1)
        view_report(args.report_id)

if __name__ == "__main__":
    main()