"""
aria2c download configuration.
All aria2c command-line options are centralized here.
"""

ARIA2C_CONFIG = {
    "max_connection_per_server": 16,
    "split": 16,
    "min_split_size": "1M",
    "continue_download": True,
    "retry_wait": 5,
    "max_tries": 5,
    "connect_timeout": 30,
    "timeout": 120,
    "check_certificate": False,
    "auto_file_renaming": False,
    "console_log_level": "error",
    "summary_interval": 0,
    "file_allocation": "none",
    "allow_overwrite": False,
}

# Timeout for subprocess (in seconds) — 30 minutes
DOWNLOAD_TIMEOUT = 1800


def build_aria2c_command(url, download_dir, filename, config=None):
    """
    Builds the aria2c CLI command from config.
    """
    cfg = config or ARIA2C_CONFIG

    cmd = [
        "aria2c",
        f"--max-connection-per-server={cfg['max_connection_per_server']}",
        f"--split={cfg['split']}",
        f"--min-split-size={cfg['min_split_size']}",
        f"--continue={'true' if cfg['continue_download'] else 'false'}",
        f"--retry-wait={cfg['retry_wait']}",
        f"--max-tries={cfg['max_tries']}",
        f"--connect-timeout={cfg['connect_timeout']}",
        f"--timeout={cfg['timeout']}",
        f"--check-certificate={'true' if cfg['check_certificate'] else 'false'}",
        f"--auto-file-renaming={'true' if cfg['auto_file_renaming'] else 'false'}",
        f"--console-log-level={cfg['console_log_level']}",
        f"--summary-interval={cfg['summary_interval']}",
        f"--file-allocation={cfg['file_allocation']}",
        f"--allow-overwrite={'true' if cfg['allow_overwrite'] else 'false'}",
        "--dir", str(download_dir),
        "--out", filename,
        url
    ]
    return cmd
