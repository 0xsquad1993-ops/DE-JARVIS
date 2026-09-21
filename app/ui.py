from pathlib import Path
import os
import urllib.error
import urllib.request
import html
import json
import subprocess
import sys
import time
from datetime import datetime

import pandas as pd
import streamlit as st


# ============================================================
 # J.A.R.V.I.S FRONTEND v7.1.2
# Tactical HUD + Classic HUD + Production Orchestrator Monitor
# Backend: existing app.jarvis
# ============================================================

VERSION = "7.1.4"

# Intentional dashboard pacing.
# Each pipeline stage stays visible long enough to understand it.
STEP_DISPLAY_SECONDS = 2.0
STEP_COMPLETE_SECONDS = 0.8

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

METADATA_DIR = DATA_DIR / "metadata"
HISTORY_DIR = METADATA_DIR / "history"
HISTORY_FILE = HISTORY_DIR / "history.json"
INGESTION_DIR = METADATA_DIR / "ingestion"

for directory in (
    RAW_DIR,
    BRONZE_DIR,
    SILVER_DIR,
    GOLD_DIR,
    METADATA_DIR,
    HISTORY_DIR,
):
    directory.mkdir(parents=True, exist_ok=True)


st.set_page_config(
    page_title="J.A.R.V.I.S",
    page_icon="◉",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# SESSION
# ============================================================

DEFAULTS = {
    "ui_mode": "TACTICAL HUD",
    "last_command": "",
    "last_output": "",
    "last_status": "IDLE",
    "selected_file": None,
    "orchestrator_report": None,
    "sidebar_view": "OVERVIEW",
}

for key, value in DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = value


# ============================================================
# HELPERS
# ============================================================

def norm(path):
    return str(path).replace("\\", "/")


def rel(path):
    try:
        return norm(Path(path).relative_to(PROJECT_ROOT))
    except Exception:
        return norm(path)


def count_files(directory):
    if not directory.exists():
        return 0
    return sum(1 for p in directory.rglob("*") if p.is_file())


def all_files():
    result = []
    for directory in (
        RAW_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        METADATA_DIR,
    ):
        if directory.exists():
            result.extend(
                p for p in directory.rglob("*") if p.is_file()
            )
    return sorted(result, key=lambda p: str(p).lower())


def metadata_files():
    if not METADATA_DIR.exists():
        return []
    return sorted(METADATA_DIR.rglob("*.json"))


def layer_of(path):
    value = rel(path).lower()
    if "data/raw/" in value:
        return "RAW"
    if "data/bronze/" in value:
        return "BRONZE"
    if "data/silver/" in value:
        return "SILVER"
    if "data/gold/" in value:
        return "GOLD"
    if "data/metadata/" in value:
        return "METADATA"
    return "UNKNOWN"


def category_of(path):
    try:
        relative = Path(path).relative_to(METADATA_DIR)
        if len(relative.parts) > 1:
            return relative.parts[0]
    except Exception:
        pass
    return "unknown"


def read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as error:
        return {"error": str(error)}


def read_history():
    if not HISTORY_FILE.exists():
        return []
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as file:
            value = json.load(file)
        return value if isinstance(value, list) else []
    except Exception:
        return []


def save_history(record):
    history = read_history()
    history.append(record)
    with open(HISTORY_FILE, "w", encoding="utf-8") as file:
        json.dump(
            history,
            file,
            indent=2,
            ensure_ascii=False,
            default=str,
        )


CHUNK_SIZE = 16 * 1024 * 1024


def stream_uploaded_file(uploaded, output_path, chunk_size=CHUNK_SIZE):
    """Write an uploaded file incrementally instead of buffering it in RAM."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    uploaded.seek(0)
    written = 0
    with open(output_path, "wb") as target:
        while True:
            chunk = uploaded.read(chunk_size)
            if not chunk:
                break
            target.write(chunk)
            written += len(chunk)
    return written


def save_ingestion_record(file_path, uploaded_size):
    """
    Store frontend-upload metadata in the backend metadata layer.
    The actual CSV is stored under data/raw.
    """
    INGESTION_DIR.mkdir(parents=True, exist_ok=True)

    file_path = Path(file_path)
    record = {
        "ingestion_version": VERSION,
        "status": "STORED",
        "file_name": file_path.name,
        "raw_path": rel(file_path),
        "size_bytes": uploaded_size,
        "ingested_at": datetime.now().isoformat(timespec="seconds"),
        "source": "STREAMLIT_RAW_UPLOAD",
        "pipeline_trigger": "JARVIS_BACKEND",
    }

    metadata_path = INGESTION_DIR / f"{file_path.stem}.json"

    with open(metadata_path, "w", encoding="utf-8") as file:
        json.dump(
            record,
            file,
            indent=2,
            ensure_ascii=False,
        )

    return metadata_path


def extract_target(command):
    for word in command.split():
        word = word.strip("\"',")
        if word.lower().endswith(".csv"):
            return word
    return None


PIPELINE_STEPS = [
    ("↓", "SOURCE", "SOURCE"),
    ("◇", "SCHEMA", "SCHEMA"),
    ("◎", "SEMANTIC", "SEMANTIC"),
    ("△", "QUALITY", "QUALITY"),
    ("✓", "VALIDATE", "VALIDATION"),
    ("⚙", "TRANSFORM", "TRANSFORMATION"),
    ("□", "SILVER", "SILVER"),
    ("◆", "GOLD", "GOLD"),
]


def new_pipeline_status():
    return {keyword: "STANDBY" for _, _, keyword in PIPELINE_STEPS}



def latest_orchestrator_report(target=None):
    """Load the newest v7.1.2 production orchestrator execution report."""
    reports = sorted(
        METADATA_DIR.glob("execution_EXEC_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    target_name = Path(target).name.lower() if target else None
    for report_path in reports:
        try:
            report = read_json(report_path)
            if not isinstance(report, dict):
                continue
            source = str(report.get("source", ""))
            if target_name and Path(source).name.lower() != target_name:
                continue
            return report
        except Exception:
            continue
    return None


def render_orchestrator_monitor(report=None):
    """Production Orchestrator v7.1.2 execution monitor."""
    if report is None:
        report = st.session_state.get("orchestrator_report")
        if report is None:
            target = extract_target(st.session_state.get("last_command", ""))
            report = latest_orchestrator_report(target)

    st.html("""
    <div style="
        margin-top:26px;
        padding:12px 14px;
        border-left:3px solid #4bd7ff;
        background:linear-gradient(90deg,rgba(75,215,255,.08),rgba(0,0,0,0));
        color:#9bdff5;
        font:10px Consolas,monospace;
        letter-spacing:2px;
    ">
        ◈ PRODUCTION ORCHESTRATOR v9.0.0
        <span style="color:#65737d;">EXECUTION / RETRY / ARTIFACT / ERROR TELEMETRY</span>
    </div>
    """)

    if not report:
        st.caption("No production orchestrator execution report found yet.")
        return

    status = str(report.get("status", "UNKNOWN"))
    status_upper = status.upper()
    status_text = "● SUCCESS" if status_upper == "SUCCESS" else ("● FAILED" if status_upper == "FAILED" else f"● {status_upper}")

    execution_id = report.get("execution_id", "N/A")
    duration = report.get("duration_seconds", 0)
    retries = report.get("retries", 0)
    artifacts = report.get("artifacts", []) or []
    errors = report.get("errors", []) or []
    steps = report.get("steps", []) or []
    total_rows = report.get("total_rows")
    final_rows = report.get("final_rows")

    a, b, c, d, e = st.columns(5)
    a.metric("STATUS", status_text)
    b.metric("EXECUTION ID", execution_id)
    c.metric("DURATION", f"{float(duration):.3f}s")
    d.metric("RETRIES", retries)
    e.metric("ARTIFACTS", len(artifacts))

    f, g = st.columns(2)
    with f:
        st.metric("SOURCE ROWS", total_rows if total_rows is not None else "N/A")
    with g:
        st.metric("FINAL ROWS", final_rows if final_rows is not None else "N/A")

    if steps:
        rows = []
        for step in steps:
            rows.append({
                "STEP": step.get("name", ""),
                "STATUS": step.get("status", ""),
                "ATTEMPTS": step.get("attempts", 0),
                "DURATION": f"{float(step.get('duration_seconds', 0)):.3f}s",
                "ROWS IN": step.get("rows_in", ""),
                "ROWS OUT": step.get("rows_out", ""),
                "ARTIFACT": step.get("artifact", "") or "",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    if errors:
        st.error(f"Orchestrator captured {len(errors)} error event(s).")
        with st.expander("VIEW ORCHESTRATOR ERRORS", expanded=False):
            st.json(errors)

    if artifacts:
        with st.expander("VIEW ORCHESTRATOR ARTIFACTS", expanded=False):
            for artifact in artifacts:
                st.code(str(artifact), language="text")

    with st.expander("VIEW EXECUTION REPORT JSON", expanded=False):
        st.json(report)

def _extract_v9_step(line):
    """Return a v9 orchestrator step name from a log line."""
    for _, _, keyword in PIPELINE_STEPS:
        if f"step={keyword}" in line:
            return keyword
    return None


def run_backend(command, live_placeholder, pipeline_placeholder):
    if not command.strip():
        st.warning("Enter a JARVIS command.")
        return

    command = command.strip()
    st.session_state.last_command = command
    st.session_state.last_status = "RUNNING"

    started = datetime.now()
    lines = []
    pipeline_status = new_pipeline_status()
    current_step = None

    render_pipeline(pipeline_status, pipeline_placeholder)

    try:
        # KEEP THE EXISTING J.A.R.V.I.S UI.
        # Only the backend execution path is connected to the verified v9
        # orchestrator. No HUD/layout/table redesign is performed here.
        target = extract_target(command)
        if not target:
            target = st.session_state.get("selected_file")

        source_path = None
        if target:
            candidate = Path(target)
            if not candidate.is_absolute():
                candidate = PROJECT_ROOT / target
            if candidate.exists():
                source_path = candidate
            else:
                raw_candidate = RAW_DIR / Path(target).name
                if raw_candidate.exists():
                    source_path = raw_candidate

        if source_path is not None:
            process_command = [
                sys.executable,
                str(PROJECT_ROOT / "orchestrator" / "orchestrator.py"),
                str(source_path),
                "--full",
                "--retries",
                "0",
            ]
        else:
            # Preserve the original JARVIS command path when no CSV target
            # can be resolved.
            process_command = [
                sys.executable,
                "-m",
                "app.jarvis",
                command,
            ]

        process = subprocess.Popen(
            process_command,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        while True:
            line = process.stdout.readline()
            if line == "" and process.poll() is not None:
                break

            if not line:
                continue

            line = line.rstrip()
            lines.append(line)

            live_placeholder.code(
                "\n".join(lines[-40:]),
                language="text",
            )

            # ========================================================
            # v9 LIVE PIPELINE STATE
            # v9 emits:
            #   STEP START   ... step=SOURCE
            #   STEP SUCCESS ... step=SOURCE
            #   STEP FAILED  ... step=SOURCE
            # ========================================================
            step = _extract_v9_step(line)

            if "STEP START" in line and step:
                current_step = step
                current_index = [
                    item[2] for item in PIPELINE_STEPS
                ].index(step)

                for index, (_, _, name) in enumerate(PIPELINE_STEPS):
                    if index < current_index:
                        pipeline_status[name] = "COMPLETE"
                    elif index == current_index:
                        pipeline_status[name] = "RUNNING"
                    else:
                        pipeline_status[name] = "STANDBY"

                render_pipeline(
                    pipeline_status,
                    pipeline_placeholder,
                )
                time.sleep(STEP_DISPLAY_SECONDS)

            elif "STEP SUCCESS" in line and step:
                pipeline_status[step] = "COMPLETE"
                current_step = step
                render_pipeline(
                    pipeline_status,
                    pipeline_placeholder,
                )
                time.sleep(STEP_COMPLETE_SECONDS)

            elif "STEP FAILED" in line and step:
                pipeline_status[step] = "FAILED"
                current_step = step
                render_pipeline(
                    pipeline_status,
                    pipeline_placeholder,
                )

            elif "EXECUTION SUCCESS" in line:
                for _, _, name in PIPELINE_STEPS:
                    pipeline_status[name] = "COMPLETE"
                render_pipeline(
                    pipeline_status,
                    pipeline_placeholder,
                )

            elif "EXECUTION FAILED" in line:
                if current_step:
                    pipeline_status[current_step] = "FAILED"
                render_pipeline(
                    pipeline_status,
                    pipeline_placeholder,
                )

        return_code = process.wait()

        output = "\n".join(lines)
        target = extract_target(command) or st.session_state.get("selected_file")

        # IMPORTANT: v9 writes the authoritative JSON execution report.
        # Do not use process return code as the pipeline status because the
        # v9 CLI prints the report and does not explicitly sys.exit(1).
        orchestrator_report = latest_orchestrator_report(target)
        st.session_state.orchestrator_report = orchestrator_report

        if orchestrator_report:
            status = str(
                orchestrator_report.get("status", "UNKNOWN")
            ).upper()

            if status == "SUCCESS":
                for _, _, name in PIPELINE_STEPS:
                    pipeline_status[name] = "COMPLETE"
            elif status == "FAILED":
                failed_found = False
                for step in orchestrator_report.get("steps", []) or []:
                    name = str(step.get("name", ""))
                    step_status = str(step.get("status", "")).upper()
                    if name in pipeline_status:
                        if step_status == "SUCCESS":
                            pipeline_status[name] = "COMPLETE"
                        elif step_status == "FAILED":
                            pipeline_status[name] = "FAILED"
                            failed_found = True
                if not failed_found and current_step:
                    pipeline_status[current_step] = "FAILED"
            else:
                status = "SUCCESS" if return_code == 0 else "FAILED"
        else:
            # Fallback for the legacy app.jarvis path.
            status = "SUCCESS" if return_code == 0 else "FAILED"
            if status == "SUCCESS":
                for _, _, name in PIPELINE_STEPS:
                    pipeline_status[name] = "COMPLETE"
            elif current_step:
                pipeline_status[current_step] = "FAILED"

        render_pipeline(
            pipeline_status,
            pipeline_placeholder,
        )

        record = {
            "run_id": "RUN-" + started.strftime("%Y%m%d-%H%M%S"),
            "timestamp": started.isoformat(timespec="seconds"),
            "command": command,
            "target": target,
            "status": status,
            "return_code": return_code,
            "execution_id": (
                orchestrator_report.get("execution_id")
                if orchestrator_report else None
            ),
            "silver_path": (
                rel(SILVER_DIR / target)
                if target and (SILVER_DIR / target).exists()
                else None
            ),
            "gold_path": (
                rel(GOLD_DIR / target)
                if target and (GOLD_DIR / target).exists()
                else None
            ),
            "execution_log": output,
        }

        save_history(record)

        st.session_state.last_output = output
        st.session_state.last_status = status
        st.rerun()

    except Exception as error:
        st.session_state.last_output = str(error)
        st.session_state.last_status = "FAILED"
        st.error(str(error))


# ============================================================
# CSS
# ============================================================

st.html(
    r"""
<style>

:root {
    --black: #000000;
    --black2: #020304;
    --panel: #050709;
    --panel2: #080b0e;
    --line: #172027;
    --cyan: #4bd7ff;
    --cyan2: #138dba;
    --red: #e3293b;
    --red2: #74121c;
    --green: #37e58b;
    --yellow: #f5c84c;
    --text: #e9f0f4;
    --muted: #65737d;
}

html, body, .stApp {
    background: #000 !important;
    color: var(--text) !important;
}

.stApp {
    background:
        radial-gradient(circle at 50% 15%, rgba(14, 73, 94, .10), transparent 34%),
        #000 !important;
}

.block-container {
    max-width: 1850px !important;
    padding: 14px 22px 60px !important;
}

[data-testid="stHeader"],
[data-testid="stDecoration"] {
    background: #000 !important;
}

[data-testid="stSidebar"] {
    background: #010203 !important;
    border-right: 1px solid #11191e !important;
}

[data-testid="stSidebar"] * {
    color: #c9d3d9 !important;
}

.stTextInput input,
[data-baseweb="select"] > div {
    background: #030405 !important;
    color: #eef4f7 !important;
    border-color: #1b252b !important;
}

.stTextInput input:focus {
    border-color: var(--cyan) !important;
    box-shadow: 0 0 14px rgba(75,215,255,.10) !important;
}

.stButton > button {
    background: #030405 !important;
    color: #dce6eb !important;
    border: 1px solid #1d282e !important;
    border-radius: 7px !important;
    font-weight: 800 !important;
}

.stButton > button:hover {
    color: white !important;
    border-color: var(--cyan) !important;
    box-shadow: 0 0 16px rgba(75,215,255,.10) !important;
}

[data-testid="stMetric"] {
    background: #030405 !important;
    border: 1px solid #111a20 !important;
}

[data-testid="stExpander"] {
    background: #020304 !important;
    border: 1px solid #121a1f !important;
}

[data-testid="stDataFrame"] {
    border: 1px solid #121a1f !important;
}


/* ============================================================
   GOLD LAYER UI v5.8
   ============================================================ */

.gold-layer-header {
    display:flex;
    justify-content:space-between;
    align-items:center;
    margin:12px 0 16px;
    padding:16px 18px;
    border:1px solid #1b3039;
    background:linear-gradient(90deg,#03080b,#061117);
}
.gold-layer-title {
    color:#e9f0f4;
    font:900 18px Consolas,monospace;
    letter-spacing:3px;
}
.gold-layer-subtitle {
    margin-top:5px;
    color:#61737d;
    font:8px Consolas,monospace;
    letter-spacing:1.5px;
}
.gold-layer-counts {
    display:flex;
    gap:18px;
    color:#71838c;
    font:8px Consolas,monospace;
    letter-spacing:1px;
}
.gold-layer-counts b { color:#4bd7ff; }

.gold-section-label {
    margin:18px 0 8px;
    padding:9px 12px;
    border-left:3px solid #4bd7ff;
    background:#03080b;
    color:#b9dce8;
    font:900 10px Consolas,monospace;
    letter-spacing:2px;
}
.gold-section-label span {
    float:right;
    color:#596b74;
    font-weight:normal;
    font-size:8px;
}
.detail-label { border-left-color:#37e58b; }

.gold-output-card {
    margin:8px 0 2px;
    padding:16px;
    border:1px solid #172b33;
    background:#020607;
}
.gold-output-card.analytics { box-shadow:inset 3px 0 0 #4bd7ff; }
.gold-output-card.detail { box-shadow:inset 3px 0 0 #37e58b; }

.gold-card-top {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
}
.gold-card-title {
    color:#e8f1f5;
    font:900 14px Consolas,monospace;
    letter-spacing:1.5px;
}
.gold-card-subtitle {
    margin-top:5px;
    color:#5e7580;
    font:8px Consolas,monospace;
    letter-spacing:1.5px;
}
.gold-card-badge {
    padding:4px 7px;
    border:1px solid #24404b;
    color:#4bd7ff;
    font:8px Consolas,monospace;
    letter-spacing:1px;
}
.gold-card-file {
    margin-top:14px;
    color:#a9c0ca;
    font:11px Consolas,monospace;
}
.gold-card-description {
    margin-top:7px;
    color:#60737d;
    font:8px Consolas,monospace;
    line-height:1.6;
}
.gold-card-meta {
    display:flex;
    gap:28px;
    margin-top:13px;
    padding-top:10px;
    border-top:1px solid #101b20;
    color:#52636c;
    font:7px Consolas,monospace;
    letter-spacing:1px;
}
.gold-card-meta b { color:#dbe7eb; }

.gold-inspector {
    margin:10px 0;
    padding:10px 12px;
    border:1px solid #1d3944;
    background:#041014;
    font:900 10px Consolas,monospace;
    letter-spacing:2px;
}
.gold-inspector.analytics { color:#4bd7ff; }
.gold-inspector.detail { color:#37e58b; }

/* ============================================================
   TACTICAL FRAME
   ============================================================ */

.tactical {
    position: relative;
    overflow: hidden;
    min-height: 470px;
    border: 1px solid #17242b;
    background:
        linear-gradient(rgba(75,215,255,.025) 1px, transparent 1px),
        linear-gradient(90deg, rgba(75,215,255,.025) 1px, transparent 1px),
        #010203;
    background-size: 32px 32px;
}

.tactical::before {
    content: "";
    position: absolute;
    inset: 0;
    pointer-events: none;
    background:
        linear-gradient(90deg, transparent 49.8%, rgba(75,215,255,.05) 50%, transparent 50.2%),
        linear-gradient(0deg, transparent 49.8%, rgba(75,215,255,.05) 50%, transparent 50.2%);
}

.corner {
    position: absolute;
    width: 65px;
    height: 65px;
    border-color: var(--cyan);
    opacity: .55;
    pointer-events: none;
}

.corner.tl {
    left: 10px;
    top: 10px;
    border-left: 2px solid;
    border-top: 2px solid;
}

.corner.tr {
    right: 10px;
    top: 10px;
    border-right: 2px solid;
    border-top: 2px solid;
}

.corner.bl {
    left: 10px;
    bottom: 10px;
    border-left: 2px solid;
    border-bottom: 2px solid;
}

.corner.br {
    right: 10px;
    bottom: 10px;
    border-right: 2px solid;
    border-bottom: 2px solid;
}

/* ============================================================
   TOP BAR
   ============================================================ */

.topbar {
    position: relative;
    z-index: 5;
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 58px;
    padding: 0 16px;
    border-bottom: 1px solid #172027;
    background: #020304;
}

.brand {
    font-size: 18px;
    font-weight: 900;
    letter-spacing: 5px;
}

.brand span {
    color: var(--cyan);
}

.top-stat {
    color: #7e8b93;
    font: 8px Consolas, monospace;
    letter-spacing: 1px;
}

.top-stat b {
    color: var(--cyan);
}

/* ============================================================
   CENTRAL HUD
   ============================================================ */

.hud-layout {
    position: relative;
    z-index: 3;
    display: grid;
    grid-template-columns: 210px 1fr 210px;
    min-height: 405px;
}

.hud-side {
    padding: 28px 18px;
}

.hud-side.left {
    border-right: 1px solid #10191e;
}

.hud-side.right {
    border-left: 1px solid #10191e;
}

.side-title {
    color: #63737c;
    font: 8px Consolas, monospace;
    letter-spacing: 2px;
    margin-bottom: 15px;
}

.side-line {
    display: flex;
    justify-content: space-between;
    padding: 9px 0;
    border-bottom: 1px solid #0c1317;
    color: #65747d;
    font: 8px Consolas, monospace;
}

.side-line b {
    color: #dce6eb;
}

.side-line .cyan {
    color: var(--cyan);
}

.side-line .green {
    color: var(--green);
}

.side-line .red {
    color: var(--red);
}

/* ============================================================
   CORE BODY
   ============================================================ */

.core-zone {
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
}

.core-grid {
    position: absolute;
    width: 390px;
    height: 330px;
    opacity: .55;
    background:
        linear-gradient(rgba(75,215,255,.07) 1px, transparent 1px),
        linear-gradient(90deg, rgba(75,215,255,.07) 1px, transparent 1px);
    background-size: 20px 20px;
    mask-image: radial-gradient(circle, black 30%, transparent 75%);
}

.robot {
    position: relative;
    width: 300px;
    height: 330px;
    z-index: 4;
    filter: drop-shadow(0 0 18px rgba(75,215,255,.08));
}

/* helmet */
.helmet {
    position: absolute;
    top: 18px;
    left: 68px;
    width: 164px;
    height: 142px;
    border: 2px solid #258fb0;
    border-radius: 48% 48% 44% 44%;
    background:
        linear-gradient(145deg, #0a1115, #010203 65%);
    box-shadow:
        inset 0 0 30px rgba(75,215,255,.05),
        0 0 20px rgba(75,215,255,.08);
}

.helmet::before {
    content: "";
    position: absolute;
    left: 20px;
    right: 20px;
    top: 13px;
    bottom: 12px;
    border: 1px solid #183943;
    border-radius: 45%;
}

.eye {
    position: absolute;
    top: 61px;
    width: 45px;
    height: 14px;
    background: #67e3ff;
    box-shadow: 0 0 8px #36b9dc, 0 0 24px rgba(54,185,220,.45);
    transform: skewY(-10deg);
}

.eye.left {
    left: 27px;
}

.eye.right {
    right: 27px;
}

.jaw {
    position: absolute;
    left: 92px;
    top: 132px;
    width: 116px;
    height: 62px;
    border-left: 2px solid #258fb0;
    border-right: 2px solid #258fb0;
    border-bottom: 2px solid #258fb0;
    border-radius: 0 0 42px 42px;
    background: #030608;
}

.mouth {
    position: absolute;
    left: 120px;
    top: 163px;
    width: 60px;
    height: 3px;
    background: #a92130;
    box-shadow: 0 0 7px rgba(227,41,59,.5);
}

/* shoulders */
.shoulders {
    position: absolute;
    top: 175px;
    left: 28px;
    width: 244px;
    height: 92px;
    border: 2px solid #1e7188;
    border-radius: 48px 48px 22px 22px;
    background: linear-gradient(180deg, #071014, #010203);
}

.shoulders::before,
.shoulders::after {
    content: "";
    position: absolute;
    top: 10px;
    width: 58px;
    height: 58px;
    border: 1px solid #a01e2c;
    border-radius: 50%;
}

.shoulders::before {
    left: 18px;
}

.shoulders::after {
    right: 18px;
}

/* chest */
.chest {
    position: absolute;
    left: 79px;
    top: 210px;
    width: 142px;
    height: 104px;
    border-left: 2px solid #258fb0;
    border-right: 2px solid #258fb0;
    border-bottom: 2px solid #258fb0;
    clip-path: polygon(18% 0,82% 0,100% 100%,0 100%);
    background: #04090c;
}

.reactor {
    position: absolute;
    left: 118px;
    top: 229px;
    width: 64px;
    height: 64px;
    border-radius: 50%;
    border: 2px solid #55dfff;
    background:
        radial-gradient(circle, #bdf6ff 0 6%, #40cce9 8% 18%, #0a2028 25% 50%, #020405 53%);
    box-shadow:
        0 0 12px #34b9d7,
        0 0 38px rgba(52,185,215,.25);
    animation: reactorPulse 2.1s infinite ease-in-out;
}

.reactor::after {
    content: "";
    position: absolute;
    inset: -10px;
    border-radius: 50%;
    border: 1px dashed rgba(75,215,255,.35);
    animation: rotate 7s linear infinite;
}

@keyframes reactorPulse {
    50% { transform: scale(1.06); }
}

@keyframes rotate {
    to { transform: rotate(360deg); }
}

/* ============================================================
   RADAR
   ============================================================ */

.radar {
    width: 145px;
    height: 145px;
    margin: 5px auto 25px;
    border-radius: 50%;
    border: 1px solid #245465;
    background:
        radial-gradient(circle, transparent 0 20%, rgba(75,215,255,.06) 21% 22%, transparent 23% 40%, rgba(75,215,255,.06) 41% 42%, transparent 43%),
        linear-gradient(90deg, transparent 49.5%, rgba(75,215,255,.16) 50%, transparent 50.5%),
        linear-gradient(0deg, transparent 49.5%, rgba(75,215,255,.16) 50%, transparent 50.5%);
    position: relative;
}

.radar::before {
    content: "";
    position: absolute;
    width: 50%;
    height: 2px;
    top: 50%;
    left: 50%;
    transform-origin: left;
    background: linear-gradient(90deg, var(--cyan), transparent);
    animation: sweep 3s linear infinite;
}

.radar-dot {
    position: absolute;
    width: 5px;
    height: 5px;
    border-radius: 50%;
    background: var(--red);
    box-shadow: 0 0 8px var(--red);
}

.radar-dot.one { left: 35px; top: 45px; }
.radar-dot.two { right: 28px; top: 72px; }
.radar-dot.three { left: 70px; bottom: 25px; }

@keyframes sweep {
    to { transform: rotate(360deg); }
}

/* ============================================================
   CORE LABEL
   ============================================================ */

.core-label {
    position: absolute;
    bottom: 18px;
    left: 50%;
    transform: translateX(-50%);
    color: var(--cyan);
    font: 8px Consolas, monospace;
    letter-spacing: 4px;
}

.voice-state {
    position: absolute;
    top: 18px;
    left: 50%;
    transform: translateX(-50%);
    color: var(--green);
    font: 8px Consolas, monospace;
    letter-spacing: 3px;
}

/* ============================================================
   COMMAND BAR
   ============================================================ */

.command-panel {
    margin-top: 15px;
    padding: 13px;
    border: 1px solid #162127;
    background: #020304;
}

.command-label {
    color: var(--cyan);
    font: 8px Consolas, monospace;
    letter-spacing: 2px;
    margin-bottom: 8px;
}

/* ============================================================
   PIPELINE
   ============================================================ */

.pipeline-card {
    min-height: 85px;
    padding: 12px 5px;
    text-align: center;
    border: 1px solid #142027;
    background: #020304;
}

.pipeline-card.success {
    border-color: rgba(55,229,139,.38);
    background: rgba(5,25,17,.30);
}

.pipeline-card.running {
    border-color: rgba(75,215,255,.75);
    background: rgba(7,34,45,.45);
    box-shadow: 0 0 18px rgba(75,215,255,.16), inset 0 0 18px rgba(75,215,255,.05);
    animation: jarvisPulse 1s ease-in-out infinite alternate;
}

.pipeline-card.failed {
    border-color: rgba(227,41,59,.75);
    background: rgba(55,8,14,.42);
    box-shadow: 0 0 16px rgba(227,41,59,.12);
}

@keyframes jarvisPulse {
    from { opacity: .72; }
    to { opacity: 1; }
}

.pipeline-icon {
    font-size: 18px;
}

.pipeline-name {
    margin-top: 6px;
    color: #b9c6cc;
    font: 8px Consolas, monospace;
    letter-spacing: 1px;
}

/* ============================================================
   PIPELINE LIVE EXPLANATION
   ============================================================ */

.pipeline-status-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 7px 0 4px;
    padding: 7px 10px;
    border: 1px solid #12232b;
    background: rgba(3, 8, 11, .88);
    color: #8ca0aa;
    font: 9px Consolas;
    letter-spacing: 1.5px;
}

.pipeline-progress {
    width: 100%;
    height: 2px;
    margin-bottom: 12px;
    background: #091217;
    overflow: hidden;
}

.pipeline-progress-fill {
    height: 100%;
    background: var(--cyan);
    box-shadow: 0 0 10px rgba(75,215,255,.55);
    transition: width .12s linear;
}

.pipeline-explain {
    min-height: 25px;
    margin: 6px 0 5px;
    color: #667984;
    font: 7px Consolas;
    line-height: 1.35;
    letter-spacing: .8px;
}

.pipeline-card.running .pipeline-explain {
    color: #9bdff5;
}

.pipeline-card.success .pipeline-explain {
    color: #6fcf9c;
}

.pipeline-card.failed .pipeline-explain {
    color: #ef8790;
}

.pipeline-card.running {
    animation: pipelinePulse 1.05s ease-in-out infinite;
}

@keyframes pipelinePulse {
    0%, 100% {
        box-shadow: 0 0 0 rgba(75,215,255,0);
    }
    50% {
        box-shadow: 0 0 18px rgba(75,215,255,.18);
    }
}

.pipeline-state {
    margin-top: 5px;
    color: #59676f;
    font: 7px Consolas, monospace;
}

/* ============================================================
   EVENT LOG
   ============================================================ */

.event-log {
    max-height: 320px;
    overflow-y: auto;
    padding: 9px;
    border: 1px solid #111b20;
    background: #010203;
}

 .event-current {
    color: #dff8ff !important;
    background: rgba(75,215,255,.045);
}

.event-dot {
    display: inline-block;
    width: 14px;
    color: var(--cyan);
}

.event-dot.muted {
    color: #34434b;
}

.event-line {
    padding: 6px 8px;
    margin-bottom: 2px;
    border-left: 2px solid #1d6578;
    color: #84939c;
    font: 9px Consolas, monospace;
}

/* ============================================================
   CLASSIC
   ============================================================ */

.classic-header {
    padding: 22px;
    border: 1px solid #172027;
    background: #020304;
}

.classic-core {
    min-height: 320px;
    display: flex;
    align-items: center;
    justify-content: center;
    border: 1px solid #172027;
    background: radial-gradient(circle, rgba(75,215,255,.08), #010203 65%);
}

.classic-orb {
    width: 165px;
    height: 165px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 50%;
    border: 2px solid rgba(75,215,255,.60);
    box-shadow: 0 0 30px rgba(75,215,255,.18);
}

.classic-title {
    font-size: 30px;
    font-weight: 900;
    letter-spacing: 7px;
}

.classic-subtitle {
    color: #63737d;
    font: 9px Consolas, monospace;
    letter-spacing: 3px;
}

/* ============================================================
   FILE / FOOTER
   ============================================================ */

.file-head {
    display: grid;
    grid-template-columns: 3fr 1fr 4fr;
    gap: 10px;
    padding: 9px;
    color: #53636d;
    font: 8px Consolas, monospace;
    letter-spacing: 2px;
}

.footer {
    margin-top: 30px;
    padding: 14px;
    text-align: center;
    border-top: 1px solid #11191e;
    color: #3e4b53;
    font: 8px Consolas, monospace;
    letter-spacing: 3px;
}

</style>
"""
)


# ============================================================
# SIDEBAR
# ============================================================

def sidebar_button(label, view, icon=""):
    active = st.session_state.sidebar_view == view
    text = f"{icon} {label}".strip()
    if st.button(
        text,
        use_container_width=True,
        type="primary" if active else "secondary",
        key=f"sidebar_{view}",
    ):
        st.session_state.sidebar_view = view
        st.rerun()


with st.sidebar:
    st.markdown("# ◉ J.A.R.V.I.S")
    st.caption("TACTICAL AI DATA ENGINEERING SYSTEM")
    st.divider()

    mode = st.radio(
        "UI MODE",
        ["TACTICAL HUD", "CLASSIC HUD"],
        index=0 if st.session_state.ui_mode == "TACTICAL HUD" else 1,
        label_visibility="collapsed",
    )

    if mode != st.session_state.ui_mode:
        st.session_state.ui_mode = mode
        st.rerun()

    st.divider()

    st.markdown("### CORE")
    sidebar_button("Command Agent", "COMMAND", "🧠")
    sidebar_button("Workflow Planner", "WORKFLOW", "⚙")
    sidebar_button("Orchestrator", "ORCHESTRATOR", "◇")
    sidebar_button("Artifact Manager", "ARTIFACTS", "▣")

    st.divider()

    st.markdown("### PIPELINE CONTROL")
    sidebar_button("Run Pipeline", "RUN", "▶")
    sidebar_button("Incremental Processing", "INCREMENTAL", "⚡")
    sidebar_button("Checkpoints", "CHECKPOINTS", "⏱")
    sidebar_button("Retry / Recovery", "RETRY", "↻")
    sidebar_button("Execution History", "HISTORY", "▣")

    st.divider()

    st.markdown("### MONITORING")
    sidebar_button("Live Monitor", "LIVE", "📡")
    sidebar_button("Errors & Retries", "ERRORS", "⚠")
    sidebar_button("Pipeline Metrics", "METRICS", "📊")
    sidebar_button("Data Lineage", "LINEAGE", "🔗")

    st.divider()

    st.markdown("### DATA LAYERS")
    sidebar_button(f"RAW  {count_files(RAW_DIR)}", "RAW", "")
    sidebar_button(f"BRONZE  {count_files(BRONZE_DIR)}", "BRONZE", "")
    sidebar_button(f"SILVER  {count_files(SILVER_DIR)}", "SILVER", "")
    sidebar_button(f"GOLD  {count_files(GOLD_DIR)}", "GOLD", "")
    sidebar_button(f"METADATA  {len(metadata_files())}", "METADATA", "")

    st.divider()

    st.markdown("### PLATFORMS")
    sidebar_button("Azure", "AZURE", "☁")
    sidebar_button("Databricks", "DATABRICKS", "▣")
    sidebar_button("Microsoft Fabric", "FABRIC", "◈")
    sidebar_button("AWS", "AWS", "◎")

    st.divider()

    st.markdown("### AI")
    sidebar_button("JARVIS Brain", "BRAIN", "🧠")
    sidebar_button("Groq AI", "GROQ", "⚡")
    sidebar_button("Data Copilot", "COPILOT", "💬")

    st.divider()
    if st.button("⌂ OVERVIEW / HOME", use_container_width=True, key="sidebar_home"):
        st.session_state.sidebar_view = "OVERVIEW"
        st.rerun()

    st.success("● JARVIS ONLINE")
    st.caption(f"FRONTEND CORE v{VERSION}")
    st.caption("📤 RAW UPLOAD → BACKEND STORE → JARVIS AI")


# ============================================================
# COMMON RENDERERS
# ============================================================

def render_pipeline(statuses=None, placeholder=None):
    if statuses is None:
        output = st.session_state.last_output or ""
        statuses = new_pipeline_status()
        for _, _, keyword in PIPELINE_STEPS:
            if keyword in output:
                statuses[keyword] = "COMPLETE"

    target = placeholder if placeholder is not None else st

    with target.container():
        completed_count = sum(
            1 for value in statuses.values()
            if value == "COMPLETE"
        )
        running_steps = [
            keyword
            for keyword, value in statuses.items()
            if value == "RUNNING"
        ]
        failed_steps = [
            keyword
            for keyword, value in statuses.items()
            if value == "FAILED"
        ]

        if running_steps:
            operation_text = f"EXECUTING // {running_steps[0]}"
        elif failed_steps:
            operation_text = f"FAILED // {failed_steps[0]}"
        elif completed_count == len(PIPELINE_STEPS):
            operation_text = "PIPELINE COMPLETE // ALL SYSTEMS GREEN"
        elif completed_count > 0:
            operation_text = f"PROGRESS // {completed_count}/{len(PIPELINE_STEPS)} STEPS COMPLETE"
        else:
            operation_text = "READY // AWAITING COMMAND"

        progress_percent = int(
            (completed_count / len(PIPELINE_STEPS)) * 100
        )

        st.html(
            f'''
            <div style="margin-top:20px;color:#65737d;
                        font:8px Consolas;letter-spacing:3px;">
                PIPELINE CONTROL
            </div>
            <div class="pipeline-status-bar">
                <span>{html.escape(operation_text)}</span>
                <span>{progress_percent}%</span>
            </div>
            <div class="pipeline-progress">
                <div class="pipeline-progress-fill"
                     style="width:{progress_percent}%;"></div>
            </div>
            '''
        )

        cols = st.columns(8)

        for col, (icon, name, keyword) in zip(cols, PIPELINE_STEPS):
            state = statuses.get(keyword, "STANDBY")

            if state == "COMPLETE":
                css = "pipeline-card success"
                state_text = "✓ COMPLETE"
            elif state == "RUNNING":
                css = "pipeline-card running"
                state_text = "◉ RUNNING"
            elif state == "FAILED":
                css = "pipeline-card failed"
                state_text = "✕ FAILED"
            else:
                css = "pipeline-card"
                state_text = "○ STANDBY"

            with col:
                explanations = {
                    "SOURCE": "Read source CSV",
                    "SCHEMA": "Detect columns + types",
                    "SEMANTIC": "Understand column roles",
                    "QUALITY": "Check nulls + duplicates",
                    "VALIDATION": "Validate data integrity",
                    "TRANSFORMATION": "Clean + standardize",
                    "SILVER": "Write clean Silver data",
                    "GOLD": "Build Gold analytics",
                }

                explanation = explanations.get(
                    keyword,
                    "Pipeline operation",
                )

                st.html(
                    f"""
                    <div class="{css}">
                        <div class="pipeline-icon">{icon}</div>
                        <div class="pipeline-name">{name}</div>
                        <div class="pipeline-explain">{html.escape(explanation)}</div>
                        <div class="pipeline-state">{state_text}</div>
                    </div>
                    """
                )


def render_events():
    output = st.session_state.last_output
    if not output:
        return

    st.html('<div style="margin-top:20px;color:#65737d;font:8px Consolas;letter-spacing:3px;">LIVE EVENT STREAM</div>')

    event_lines = output.splitlines()[-30:]
    lines = []

    for index, line in enumerate(event_lines):
        escaped = html.escape(line)

        if index == len(event_lines) - 1:
            lines.append(
                f'<div class="event-line event-current">'
                f'<span class="event-dot">●</span>{escaped}'
                f'</div>'
            )
        else:
            lines.append(
                f'<div class="event-line">'
                f'<span class="event-dot muted">•</span>{escaped}'
                f'</div>'
            )

    st.html(
        '<div class="event-log">' + "".join(lines) + "</div>"
    )



def gold_type(path):
    name = Path(path).name.lower()

    # Time-series Gold must be classified before generic CSV handling.
    if name.endswith("_time_series.csv"):
        return "TIME_SERIES"

    if name.endswith("_detail.csv"):
        return "DETAIL"

    if name.endswith(".csv"):
        return "ANALYTICS"

    return "OTHER"


def gold_files():
    files = sorted(
        GOLD_DIR.glob("*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    return (
        [p for p in files if gold_type(p) == "ANALYTICS"],
        [p for p in files if gold_type(p) == "DETAIL"],
        [p for p in files if gold_type(p) == "TIME_SERIES"],
    )


def _gold_card_metrics(path, kind, preview):
    metrics = []

    try:
        if kind == "ANALYTICS":
            if "row_count" in preview.columns:
                rows = pd.to_numeric(
                    preview["row_count"], errors="coerce"
                ).fillna(0).sum()
            else:
                rows = len(preview)

            amount = None
            quantity = None

            if "amount_sum" in preview.columns:
                amount = pd.to_numeric(
                    preview["amount_sum"], errors="coerce"
                ).fillna(0).sum()

            if "quantity_sum" in preview.columns:
                quantity = pd.to_numeric(
                    preview["quantity_sum"], errors="coerce"
                ).fillna(0).sum()

            quality = "N/A"
            if "_gold_quality_status" in preview.columns and not preview.empty:
                quality = str(preview["_gold_quality_status"].iloc[0])

            metrics = [
                ("ROWS", _format_metric(rows)),
                ("AMOUNT", _format_metric(amount, "money")),
                ("QUANTITY", _format_metric(quantity)),
                ("QUALITY", quality),
            ]

        elif kind == "TIME_SERIES":
            periods = len(preview)

            latest = "N/A"
            if "period" in preview.columns and not preview.empty:
                latest = str(preview["period"].iloc[-1])

            growth = None
            trend = "N/A"

            if "amount_growth_pct" in preview.columns and not preview.empty:
                growth_values = pd.to_numeric(
                    preview["amount_growth_pct"], errors="coerce"
                ).dropna()
                if not growth_values.empty:
                    growth = float(growth_values.iloc[-1])

            if "amount_trend" in preview.columns and not preview.empty:
                trend = str(preview["amount_trend"].iloc[-1])

            metrics = [
                ("PERIODS", _format_metric(periods)),
                ("LATEST", latest),
                ("GROWTH", _format_metric(growth, "pct")),
                ("TREND", trend),
            ]

        else:
            metrics = [
                ("ROWS", _format_metric(len(preview))),
                ("COLUMNS", _format_metric(len(preview.columns))),
            ]

    except Exception:
        metrics = []

    return metrics


def render_gold_card(path, kind):
    try:
        # Gold Analytics/Detail previews stay lightweight.
        # Time-Series Gold is normally small, so reading it fully is safe.
        if kind == "TIME_SERIES":
            preview = pd.read_csv(path)
        else:
            preview = pd.read_csv(path, nrows=500)

        if kind == "DETAIL":
            icon = "📋"
            title = "DETAIL GOLD"
            subtitle = "TRANSACTION / ROW LEVEL"
            description = (
                "Original business records preserved for transaction-level "
                "analysis."
            )
            css_kind = "detail"

        elif kind == "TIME_SERIES":
            icon = "📈"
            title = "TIME SERIES GOLD"
            subtitle = "TREND / GROWTH / PERIOD INTELLIGENCE"
            description = (
                "Period-level business trends with growth percentages and "
                "trend signals."
            )
            css_kind = "timeseries"

        else:
            icon = "📊"
            title = "ANALYTICS GOLD"
            subtitle = "BUSINESS AGGREGATION"
            description = (
                "Business-level aggregated dataset with count, sum, average, "
                "minimum, maximum and KPI metrics."
            )
            css_kind = "analytics"

        size_mb = path.stat().st_size / (1024 * 1024)
        metrics = _gold_card_metrics(path, kind, preview)

        metric_html = "".join(
            f'<span>{html.escape(str(label))} '
            f'<b>{html.escape(str(value))}</b></span>'
            for label, value in metrics
        )

        st.html(f"""
        <div class="gold-output-card {css_kind}" style="
            border:1px solid #163640;
            background:linear-gradient(135deg,#02080b,#050d12);
            padding:18px 20px;
            margin:12px 0;
        ">
            <div class="gold-card-top">
                <div>
                    <div class="gold-card-title">{icon} {title}</div>
                    <div class="gold-card-subtitle">{subtitle}</div>
                </div>
                <div class="gold-card-badge">GOLD</div>
            </div>

            <div class="gold-card-file">{html.escape(path.name)}</div>

            <div class="gold-card-description">
                {html.escape(description)}
            </div>

            <div class="gold-card-meta" style="
                display:flex;
                flex-wrap:wrap;
                gap:18px;
                margin-top:14px;
            ">
                {metric_html}
                <span>SIZE <b>{size_mb:.2f} MB</b></span>
                <span>COLUMNS <b>{len(preview.columns)}</b></span>
            </div>
        </div>
        """)

        if st.button(
            f"VIEW {title}",
            key=f"gold_view_{kind}_{path.name}",
        ):
            st.session_state.selected_file = str(path)
            st.rerun()

    except Exception as error:
        st.error(f"Gold preview failed for {path.name}: {error}")



# ============================================================
# GOLD INTELLIGENCE DASHBOARD v6.0
# ============================================================

def _safe_number(value):
    try:
        return float(value)
    except Exception:
        return None


def _format_metric(value, kind="number"):
    if value is None:
        return "N/A"

    if kind == "money":
        return f"{value:,.2f}"

    if kind == "pct":
        return f"{value:,.2f}%"

    if float(value).is_integer():
        return f"{int(value):,}"

    return f"{value:,.2f}"


def _target_stem():
    target = extract_target(st.session_state.last_command or "")
    if target:
        return Path(target).stem
    return None


def _latest_time_series():
    stem = _target_stem()

    candidates = []
    if stem:
        candidates.extend(sorted(
            GOLD_DIR.glob(f"{stem}_time_series.csv"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        ))

    if candidates:
        return candidates[0]

    all_ts = sorted(
        GOLD_DIR.glob("*_time_series.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    return all_ts[0] if all_ts else None


def _latest_analytics_gold():
    stem = _target_stem()

    if stem:
        exact = GOLD_DIR / f"{stem}.csv"
        if exact.exists():
            return exact

    analytics, _, _ = gold_files()
    return analytics[0] if analytics else None


def _metric_from_df(df, preferred):
    for column in preferred:
        if column in df.columns:
            series = pd.to_numeric(df[column], errors="coerce").dropna()
            if not series.empty:
                return float(series.sum())
    return None



# ============================================================
# DECISION ENGINE v6.3
# Deterministic business signals from Gold outputs.
# ============================================================

def _decision_num(value, default=0.0):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _decision_latest_ts():
    ts_path = _latest_time_series()
    if ts_path is None:
        return None

    try:
        ts = pd.read_csv(ts_path)
        if ts.empty:
            return None
        return ts
    except Exception:
        return None


def _decision_signal_rows():
    """Build deterministic business signals from Gold analytics/time-series."""
    signals = []

    analytics_path = _latest_analytics_gold()
    ts = _decision_latest_ts()

    if ts is not None and not ts.empty:
        latest = ts.iloc[-1]

        if "amount_growth_pct" in ts.columns:
            growth = _decision_num(latest.get("amount_growth_pct"))
            if growth > 10:
                signals.append({
                    "type": "GROWTH",
                    "severity": "HIGH" if growth >= 50 else "MEDIUM",
                    "title": "Revenue acceleration",
                    "value": growth,
                    "message": f"Latest amount growth is {growth:.2f}%."
                })
            elif growth < -10:
                signals.append({
                    "type": "DECLINE",
                    "severity": "HIGH" if growth <= -30 else "MEDIUM",
                    "title": "Revenue decline",
                    "value": growth,
                    "message": f"Latest amount growth is {growth:.2f}%."
                })

        if "amount_trend" in ts.columns:
            trend = str(latest.get("amount_trend", "")).upper()
            if trend in {"UP", "DOWN"}:
                signals.append({
                    "type": "TREND",
                    "severity": "MEDIUM" if trend == "DOWN" else "INFO",
                    "title": f"Amount trend {trend}",
                    "value": trend,
                    "message": f"Latest amount trend is {trend}."
                })

        if "quantity_growth_pct" in ts.columns:
            qgrowth = _decision_num(latest.get("quantity_growth_pct"))
            if abs(qgrowth) >= 10:
                signals.append({
                    "type": "VOLUME",
                    "severity": "MEDIUM" if qgrowth < 0 else "INFO",
                    "title": "Volume movement",
                    "value": qgrowth,
                    "message": f"Latest quantity growth is {qgrowth:.2f}%."
                })

    if analytics_path is not None:
        try:
            adf = pd.read_csv(analytics_path, nrows=500)
            if "_gold_quality_status" in adf.columns and not adf.empty:
                quality = str(adf["_gold_quality_status"].iloc[0]).upper()
                if quality != "PASS":
                    signals.append({
                        "type": "QUALITY",
                        "severity": "HIGH",
                        "title": "Gold quality issue",
                        "value": quality,
                        "message": f"Gold quality status is {quality}."
                    })
        except Exception:
            pass

    return signals


def _decision_actions(signals):
    actions = []

    for signal in signals:
        stype = signal["type"]

        if stype == "GROWTH":
            actions.append(
                "Break down the latest revenue increase by city, category and product."
            )
        elif stype == "DECLINE":
            actions.append(
                "Investigate the latest revenue decline by business dimensions and data freshness."
            )
        elif stype == "TREND" and signal["value"] == "DOWN":
            actions.append(
                "Review the declining trend and identify the dimensions contributing most to the drop."
            )
        elif stype == "VOLUME":
            if _decision_num(signal["value"]) < 0:
                actions.append(
                    "Check transaction-volume drivers and investigate periods with falling quantity."
                )
            else:
                actions.append(
                    "Compare volume growth with revenue growth to validate business performance."
                )
        elif stype == "QUALITY":
            actions.append(
                "Review the Gold quality issue before using the output for downstream decisions."
            )

    return list(dict.fromkeys(actions))[:4]



# ============================================================
# AI REASONING v6.4
# Deterministic contributor analysis from Gold outputs.
# This is provider-agnostic and does not require new packages.
# ============================================================

def _reasoning_contributors():
    """Find strongest business dimensions in the latest Analytics Gold."""
    path = _latest_analytics_gold()
    if path is None:
        return []

    try:
        df = pd.read_csv(path, nrows=5000)
    except Exception:
        return []

    if df.empty:
        return []

    numeric_candidates = [
        c for c in ["amount_sum", "quantity_sum", "revenue_sum", "sales_sum"]
        if c in df.columns
    ]
    if not numeric_candidates:
        return []

    metric = numeric_candidates[0]
    numeric = pd.to_numeric(df[metric], errors="coerce")
    work = df.copy()
    work["_reasoning_metric"] = numeric
    work = work.dropna(subset=["_reasoning_metric"])

    if work.empty:
        return []

    dimension_candidates = [
        c for c in ["city", "category", "region", "department", "product"]
        if c in work.columns
    ]

    results = []

    for dimension in dimension_candidates:
        grouped = (
            work.groupby(dimension, dropna=False)["_reasoning_metric"]
            .sum()
            .sort_values(ascending=False)
        )

        if grouped.empty:
            continue

        total = float(grouped.sum())
        top_value = str(grouped.index[0])
        top_amount = float(grouped.iloc[0])
        share = (top_amount / total * 100) if total else 0.0

        results.append({
            "dimension": dimension,
            "value": top_value,
            "metric": metric,
            "amount": top_amount,
            "share_pct": share,
        })

    results.sort(key=lambda x: x["share_pct"], reverse=True)
    return results[:5]


def _reasoning_summary(signals, contributors):
    """Generate a transparent, rule-based reasoning summary."""
    growth = None
    trend = None

    for signal in signals:
        if signal["type"] in {"GROWTH", "DECLINE"}:
            growth = _decision_num(signal["value"])
        elif signal["type"] == "TREND":
            trend = str(signal["value"]).upper()

    if growth is not None and growth > 0:
        headline = f"Latest business value increased by {growth:.2f}%."
    elif growth is not None:
        headline = f"Latest business value changed by {growth:.2f}%."
    elif trend:
        headline = f"Latest business trend is {trend}."
    else:
        headline = "No strong directional signal was detected."

    if contributors:
        top = contributors[0]
        contributor_text = (
            f"The strongest visible contributor is "
            f"{top['dimension']}={top['value']}, representing "
            f"{top['share_pct']:.2f}% of the available {top['metric']} total."
        )
    else:
        contributor_text = (
            "No dimensional contributor could be calculated from the "
            "available Analytics Gold output."
        )

    return headline, contributor_text




# ============================================================
# GROQ AI ANALYST v6.9 — ACTION COPILOT
# Optional, user-triggered reasoning over verified Gold facts.
# Uses Python stdlib HTTP; no extra package required.
# ============================================================

GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"


def _load_local_env():
    """Load simple KEY=VALUE pairs from the project .env if present."""
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        return

    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        pass


def _groq_api_key():
    _load_local_env()
    return os.getenv("GROQ_API_KEY", "").strip()


def _groq_model():
    _load_local_env()
    return os.getenv(
        "GROQ_MODEL",
        "llama-3.3-70b-versatile",
    ).strip()


def _gold_breakdown(dimension, metric_candidates=None, limit=10):
    """Query a Gold Detail dataset for a verified dimension breakdown."""
    df = _investigation_detail_df()
    if df is None or dimension not in df.columns:
        return []

    metric = None
    for candidate in (metric_candidates or ["amount", "amount_sum", "revenue", "sales"]):
        if candidate in df.columns:
            metric = candidate
            break
    if metric is None:
        return []

    work = df[[dimension, metric]].copy()
    work[metric] = pd.to_numeric(work[metric], errors="coerce")
    work = work.dropna(subset=[metric])
    if work.empty:
        return []

    grouped = (
        work.groupby(dimension, dropna=False)[metric]
        .sum()
        .reset_index()
        .sort_values(metric, ascending=False)
    )
    total = float(grouped[metric].sum())
    grouped["share_pct"] = (grouped[metric] / total * 100) if total else 0.0

    rows = []
    for rank, (_, row) in enumerate(grouped.head(limit).iterrows(), start=1):
        rows.append({
            "rank": rank,
            "value": str(row[dimension]),
            "metric": metric,
            "amount": float(row[metric]),
            "share_pct": float(row["share_pct"]),
        })
    return rows


def _gold_cross_breakdown(parent_dimension, child_dimension, metric_candidates=None, limit=20):
    """Return verified parent -> child metric breakdown from Detail Gold."""
    df = _investigation_detail_df()
    if df is None or parent_dimension not in df.columns or child_dimension not in df.columns:
        return []

    metric = None
    for candidate in (metric_candidates or ["amount", "amount_sum", "revenue", "sales"]):
        if candidate in df.columns:
            metric = candidate
            break
    if metric is None:
        return []

    work = df[[parent_dimension, child_dimension, metric]].copy()
    work[metric] = pd.to_numeric(work[metric], errors="coerce")
    work = work.dropna(subset=[metric])
    if work.empty:
        return []

    grouped = (
        work.groupby([parent_dimension, child_dimension], dropna=False)[metric]
        .sum()
        .reset_index()
    )
    parent_totals = grouped.groupby(parent_dimension, dropna=False)[metric].transform("sum")
    grouped["parent_share_pct"] = (grouped[metric] / parent_totals * 100).where(parent_totals != 0, 0)
    grouped = grouped.sort_values(metric, ascending=False).head(limit)

    rows = []
    for rank, (_, row) in enumerate(grouped.iterrows(), start=1):
        rows.append({
            "rank": rank,
            "parent": str(row[parent_dimension]),
            "child": str(row[child_dimension]),
            "metric": metric,
            "amount": float(row[metric]),
            "parent_share_pct": float(row["parent_share_pct"]),
        })
    return rows


def _gold_query_intent(question=""):
    """Natural-language query planner for verified Gold retrieval.

    The planner is deterministic by design: Groq explains the retrieved facts,
    while this layer decides exactly which Gold rows/aggregations are allowed.
    """
    q = str(question or "").lower().strip()
    dimensions = ["product", "category", "city", "region", "department"]

    mentioned = [d for d in dimensions if d in q]

    limit = 10
    if any(x in q for x in ["top 3", "top three"]):
        limit = 3
    elif any(x in q for x in ["top 5", "top five"]):
        limit = 5
    elif any(x in q for x in ["top 10", "top ten"]):
        limit = 10
    elif any(x in q for x in ["bottom 3", "bottom three"]):
        limit = 3
    elif any(x in q for x in ["bottom 5", "bottom five"]):
        limit = 5

    metric = "amount"
    if any(x in q for x in ["quantity", "volume", "units", "unit sold", "units sold"]):
        metric = "quantity"
    elif any(x in q for x in ["revenue", "sales", "amount", "value", "business value"]):
        metric = "amount"

    if any(x in q for x in ["compare", "versus", " vs ", "difference between"]):
        mode = "COMPARE"
    elif any(x in q for x in ["trend", "over time", "monthly", "month by month", "latest period", "growth"]):
        mode = "TIME_SERIES"
    elif any(x in q for x in ["why", "reason", "cause", "explain"]):
        mode = "REASONING"
    elif any(x in q for x in ["top ", "bottom ", "highest", "lowest", "most", "least"]):
        mode = "RANKING"
    else:
        mode = "BREAKDOWN"

    child = None
    for d in dimensions:
        if f"by {d}" in q or f"per {d}" in q or f"for each {d}" in q:
            child = d
            break

    parent = None
    for d in dimensions:
        if d in q and d != child:
            parent = d
            break

    return {
        "mode": mode,
        "metric": metric,
        "mentioned_dimensions": mentioned,
        "parent_dimension": parent,
        "child_dimension": child,
        "limit": limit,
    }


def _gold_dimension_values():
    """Return unique dimension values from the current Detail Gold file."""
    df = _investigation_detail_df()
    if df is None:
        return {}

    values = {}
    for dimension in ["product", "category", "city", "region", "department"]:
        if dimension not in df.columns:
            continue
        raw = df[dimension].dropna().astype(str).str.strip()
        values[dimension] = sorted(
            {v for v in raw if v},
            key=lambda x: (-len(x), x.lower()),
        )
    return values


def _gold_extract_filters(question=""):
    """Resolve concrete dimension values mentioned in natural-language questions."""
    q = str(question or "").lower()
    values = _gold_dimension_values()
    filters = {}

    # Longest-first avoids partial matches such as "New York" vs "York".
    for dimension, candidates in values.items():
        for value in candidates:
            if value.lower() in q:
                filters[dimension] = value
                break
    return filters


def _gold_metric_column(df, metric="amount"):
    candidates = {
        "amount": ["amount", "amount_sum", "revenue", "sales", "value"],
        "quantity": ["quantity", "quantity_sum", "units", "volume"],
    }
    for candidate in candidates.get(metric, candidates["amount"]):
        if candidate in df.columns:
            return candidate
    return None


def _gold_filtered_rows(filters=None):
    df = _investigation_detail_df()
    if df is None:
        return None

    work = df.copy()
    for dimension, value in (filters or {}).items():
        if dimension not in work.columns:
            continue
        work = work[
            work[dimension].astype(str).str.strip().str.lower()
            == str(value).strip().lower()
        ]
    return work if not work.empty else None


def _gold_filtered_breakdown(dimension, filters=None, metric="amount", limit=10, bottom=False):
    """Verified breakdown after applying exact dimension filters."""
    work = _gold_filtered_rows(filters)
    if work is None or dimension not in work.columns:
        return []

    metric_column = _gold_metric_column(work, metric)
    if metric_column is None:
        return []

    data = work[[dimension, metric_column]].copy()
    data[metric_column] = pd.to_numeric(data[metric_column], errors="coerce")
    data = data.dropna(subset=[metric_column])
    if data.empty:
        return []

    grouped = (
        data.groupby(dimension, dropna=False)[metric_column]
        .sum()
        .reset_index()
    )
    grouped = grouped.sort_values(metric_column, ascending=bottom)
    total = float(grouped[metric_column].sum())
    grouped["share_pct"] = (grouped[metric_column] / total * 100) if total else 0.0

    rows = []
    for rank, (_, row) in enumerate(grouped.head(limit).iterrows(), start=1):
        rows.append({
            "rank": rank,
            "value": str(row[dimension]),
            "dimension": dimension,
            "metric": metric,
            "metric_column": metric_column,
            "amount": float(row[metric_column]),
            "share_pct": float(row["share_pct"]),
        })
    return rows


def _gold_filtered_summary(filters=None, metric="amount"):
    work = _gold_filtered_rows(filters)
    if work is None:
        return None
    metric_column = _gold_metric_column(work, metric)
    if metric_column is None:
        return None
    series = pd.to_numeric(work[metric_column], errors="coerce").dropna()
    if series.empty:
        return None
    return {
        "filters": filters or {},
        "metric": metric,
        "metric_column": metric_column,
        "row_count": int(len(work)),
        "total": float(series.sum()),
        "average": float(series.mean()),
        "minimum": float(series.min()),
        "maximum": float(series.max()),
    }


def _gold_compare_values(question="", metric="amount"):
    """Compare concrete Gold dimension values named in the question."""
    q = str(question or "").lower()
    values = _gold_dimension_values()
    matches = []
    for dimension, candidates in values.items():
        found = [v for v in candidates if v.lower() in q]
        if len(found) >= 2:
            for value in found[:10]:
                summary = _gold_filtered_summary({dimension: value}, metric)
                if summary:
                    matches.append({
                        "dimension": dimension,
                        "value": value,
                        **summary,
                    })
            if matches:
                return matches
    return matches


def _gold_time_series_from_detail(filters=None, metric="amount", limit=24):
    """Build a verified period trend directly from Detail Gold when possible."""
    work = _gold_filtered_rows(filters)
    if work is None:
        return []

    date_column = None
    preferred = [
        "date", "datetime", "timestamp", "created_at", "updated_at",
        "transaction_date", "order_date", "event_date", "sale_date", "invoice_date",
    ]
    for candidate in preferred:
        if candidate in work.columns:
            date_column = candidate
            break
    if date_column is None:
        for column in work.columns:
            low = str(column).lower()
            if low.endswith("_date") or low.endswith("_datetime") or low.endswith("_timestamp"):
                date_column = column
                break
    if date_column is None:
        return []

    metric_column = _gold_metric_column(work, metric)
    if metric_column is None:
        return []

    dates = pd.to_datetime(work[date_column], errors="coerce")
    data = pd.DataFrame({"date": dates, "metric": pd.to_numeric(work[metric_column], errors="coerce")})
    data = data.dropna(subset=["date", "metric"])
    if data.empty:
        return []
    data["period"] = data["date"].dt.to_period("M").astype(str)
    grouped = data.groupby("period", as_index=False)["metric"].sum().sort_values("period")
    grouped["growth_pct"] = grouped["metric"].pct_change() * 100
    grouped["growth_pct"] = grouped["growth_pct"].round(2)
    return grouped.tail(limit).to_dict(orient="records")


def _gold_query_data(question=""):
    """v7.0 Natural Language Gold Query Engine.

    Converts flexible questions into a deterministic retrieval plan, applies
    exact filters against Detail Gold, and sends only verified results to Groq.
    """
    intent = _gold_query_intent(question)
    filters = _gold_extract_filters(question)
    q = str(question or "").lower()
    metric = intent.get("metric", "amount")
    bottom = "bottom" in q or "lowest" in q or "least" in q

    data = {
        "query_intent": intent,
        "query_plan": {
            "operation": intent.get("mode"),
            "metric": metric,
            "filters": filters,
            "limit": intent.get("limit", 10),
        },
    }

    if filters:
        summary = _gold_filtered_summary(filters, metric)
        if summary:
            data["filtered_summary"] = summary

    compare = _gold_compare_values(question, metric)
    if compare:
        data["comparison"] = compare

    dimensions = ["product", "category", "city", "region", "department"]
    requested = [d for d in dimensions if d in q]

    # Ranking/breakdown: if no dimension is explicit, infer product for product
    # questions and otherwise use the most useful available dimension.
    if not requested:
        if "product" in q:
            requested = ["product"]
        elif any(x in q for x in ["city", "location", "region"]):
            requested = ["city"]
        elif "category" in q:
            requested = ["category"]
        else:
            requested = ["product", "category", "city"]

    for dimension in requested:
        dimension_filters = dict(filters)
        dimension_filters.pop(dimension, None)
        rows = _gold_filtered_breakdown(
            dimension,
            dimension_filters,
            metric=metric,
            limit=intent.get("limit", 10),
            bottom=bottom,
        )
        if rows:
            data[f"{dimension}_breakdown"] = rows

    parent = intent.get("parent_dimension")
    child = intent.get("child_dimension")
    if parent and child:
        cross_filters = dict(filters)
        cross_filters.pop(parent, None)
        cross_filters.pop(child, None)
        parent_work = _gold_filtered_rows(cross_filters)
        if parent_work is not None and parent in parent_work.columns and child in parent_work.columns:
            metric_column = _gold_metric_column(parent_work, metric)
            if metric_column:
                temp = parent_work[[parent, child, metric_column]].copy()
                temp[metric_column] = pd.to_numeric(temp[metric_column], errors="coerce")
                temp = temp.dropna(subset=[metric_column])
                grouped = temp.groupby([parent, child], dropna=False)[metric_column].sum().reset_index()
                grouped = grouped.sort_values(metric_column, ascending=False).head(50)
                data[f"{parent}_by_{child}"] = [
                    {
                        "parent": str(row[parent]),
                        "child": str(row[child]),
                        "metric": metric,
                        "amount": float(row[metric_column]),
                    }
                    for _, row in grouped.iterrows()
                ]

    if intent.get("mode") == "TIME_SERIES" or any(x in q for x in ["monthly", "over time", "trend"]):
        trend = _gold_time_series_from_detail(filters, metric)
        if trend:
            data["detail_time_series"] = trend

        ts_path = _latest_time_series()
        if ts_path is not None:
            try:
                ts = pd.read_csv(ts_path)
                if not ts.empty:
                    data["latest_time_series"] = ts.iloc[-1].to_dict()
                    data["time_series_periods"] = int(len(ts))
            except Exception:
                pass

    detail = _investigation_detail_df()
    if detail is not None:
        data["detail_columns"] = list(detail.columns)
        data["detail_row_count"] = int(len(detail))

    return data

def _deterministic_next_action(question, query_data):
    """Generate a safe, deterministic next investigation action from verified query metadata."""
    q = str(question or "").lower()
    intent = query_data.get("query_intent", {}) if isinstance(query_data, dict) else {}
    mode = intent.get("mode")

    if mode == "CROSS_BREAKDOWN":
        return "Review the returned cross-breakdown and investigate the highest-value segment first."
    if "top" in q or "bottom" in q:
        return "Inspect the returned ranking, then drill into the leading or trailing segment by city/category/product."
    if mode == "TIME_SERIES":
        return "Compare the latest period with the prior periods and investigate the largest growth or decline contributor."
    if any(x in q for x in ["why", "reason", "cause", "explain"]):
        return "Drill down by product, city, and category, then compare volume growth against amount growth."
    return "Use the verified Gold result as the starting point for the next drill-down."



def _build_groq_context(question=""):
    """Create a verified fact pack, including question-relevant Gold queries."""
    signals = _decision_signal_rows()
    contributors = _reasoning_contributors()
    query_data = _gold_query_data(question)

    context = {
        "signals": signals,
        "contributors": contributors,
        "gold_query_results": query_data,
        "analytics_file": str(_latest_analytics_gold())
        if _latest_analytics_gold() else None,
        "detail_file": str(
            GOLD_DIR / f"{_latest_analytics_gold().stem}_detail.csv"
        ) if _latest_analytics_gold() else None,
        "time_series_file": str(_latest_time_series())
        if _latest_time_series() else None,
    }

    return context


def _call_groq_followup(question, prior_report=""):
    api_key = _groq_api_key()
    if not api_key:
        return {
            "ok": False,
            "error": (
                "GROQ_API_KEY is not configured. Add GROQ_API_KEY to "
                "DE-JARVIS/.env and restart Streamlit."
            ),
        }

    context = _build_groq_context(question)
    query_data = context.get("gold_query_results", {})
    next_action = _deterministic_next_action(question, query_data)

    system_prompt = """
You are JARVIS AI Copilot for a Data Engineering dashboard.
Use ONLY the verified Gold-layer facts and Gold Query Engine results supplied below.
Never invent numbers, causes, customers, products, dates, or business events.
If a requested dimension is present in gold_query_results, use those exact verified values.
If the requested dimension is absent, explicitly say what additional Gold data is needed.
Do not claim a product/category/city ranking unless it appears in the supplied query results.
Separate observed facts from hypotheses.
Answer the user's question directly and concisely.
When useful, reference the exact metric/value and share percentage.
If a next investigation is useful, provide one concrete action based only on the supplied Gold data.
""".strip()

    user_payload = {
        "verified_gold_facts": context,
        "previous_ai_report": prior_report,
        "user_question": question,
    }

    payload = {
        "model": _groq_model(),
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    "Answer this question using the verified Gold fact pack:\n\n"
                    + json.dumps(user_payload, indent=2, default=str)
                ),
            },
        ],
        "temperature": 0.15,
        "max_tokens": 700,
    }

    request = urllib.request.Request(
        GROQ_ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "DE-JARVIS/7.0",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            body = json.loads(response.read().decode("utf-8"))

        choices = body.get("choices", [])
        if not choices:
            return {"ok": False, "error": "Groq returned no choices."}

        content = choices[0].get("message", {}).get("content", "")
        if not content:
            return {"ok": False, "error": "Groq returned an empty answer."}

        return {
            "ok": True,
            "content": content,
            "model": _groq_model(),
            "next_action": next_action,
            "query_intent": query_data.get("query_intent", {}),
        }

    except urllib.error.HTTPError as error:
        try:
            detail = error.read().decode("utf-8")
        except Exception:
            detail = str(error)
        return {
            "ok": False,
            "error": f"Groq HTTP {error.code}: {detail[:600]}",
        }
    except urllib.error.URLError as error:
        return {
            "ok": False,
            "error": f"Groq connection failed: {error.reason}",
        }
    except Exception as error:
        return {
            "ok": False,
            "error": f"Groq copilot failed: {error}",
        }


def _call_groq_analyst():
    api_key = _groq_api_key()
    if not api_key:
        return {
            "ok": False,
            "error": (
                "GROQ_API_KEY is not configured. Add GROQ_API_KEY to "
                "DE-JARVIS/.env and restart Streamlit."
            ),
        }

    context = _build_groq_context()

    system_prompt = """
You are JARVIS, a Data Engineering business analyst.

Use ONLY the verified facts supplied in the user message.
Do not invent numbers, causes, customers, products, or business events.
Distinguish observed facts from hypotheses.
If the data does not establish a cause, say that the cause requires investigation.

Return a concise report with exactly these headings:
WHAT HAPPENED
MAIN CONTRIBUTORS
WHY IT MAY BE HAPPENING
RECOMMENDED ACTION
DATA CAVEAT

Keep the report practical and suitable for a Data Engineering dashboard.
""".strip()

    user_prompt = (
        "Analyze this verified Gold-layer fact pack:\n\n"
        + json.dumps(context, indent=2, default=str)
    )

    payload = {
        "model": _groq_model(),
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 900,
    }

    request = urllib.request.Request(
        GROQ_ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "DE-JARVIS/6.8",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            body = json.loads(response.read().decode("utf-8"))

        choices = body.get("choices", [])
        if not choices:
            return {"ok": False, "error": "Groq returned no choices."}

        content = choices[0].get("message", {}).get("content", "")
        if not content:
            return {"ok": False, "error": "Groq returned an empty report."}

        return {
            "ok": True,
            "content": content,
            "model": _groq_model(),
        }

    except urllib.error.HTTPError as error:
        try:
            detail = error.read().decode("utf-8")
        except Exception:
            detail = str(error)

        return {
            "ok": False,
            "error": f"Groq HTTP {error.code}: {detail[:600]}",
        }

    except urllib.error.URLError as error:
        return {
            "ok": False,
            "error": f"Groq connection failed: {error.reason}",
        }

    except Exception as error:
        return {
            "ok": False,
            "error": f"Groq analyst failed: {error}",
        }


def render_groq_analyst():
    st.html("""
    <div style="
        margin-top:26px;
        padding:12px 14px;
        border-left:3px solid #00f0ff;
        background:linear-gradient(90deg,rgba(0,240,255,.07),rgba(0,0,0,0));
        color:#7fefff;
        font:10px Consolas;
        letter-spacing:2px;
    ">
        🧠 GROQ AI ANALYST
        <span style="color:#7f929f;">FACTS → REASONING → REPORT</span>
    </div>
    """)

    st.caption(
        "Groq receives only the verified Gold signals and contribution metrics. "
        "The call runs only when you press the button."
    )

    if st.button(
        "GENERATE AI ANALYST REPORT",
        key="groq_generate_analyst_v66",
        type="primary",
    ):
        with st.spinner("JARVIS is asking Groq to analyze the verified Gold facts..."):
            result = _call_groq_analyst()

        st.session_state["groq_analyst_result"] = result

    result = st.session_state.get("groq_analyst_result")

    if not result:
        return

    if not result.get("ok"):
        st.error(result.get("error", "Unknown Groq error"))
        return

    st.success(f"Groq Analyst • {result.get('model', 'configured model')}")

    st.markdown(result["content"])
    st.session_state["groq_last_report"] = result["content"]

    # ------------------------------------------------------------
    # AI COPILOT — interactive follow-up over the same verified facts
    # ------------------------------------------------------------
    st.markdown(
        "<div style=\"margin-top:24px;padding:12px 14px;border-left:3px solid #7c3aed;\""
        "background:linear-gradient(90deg,rgba(124,58,237,.10),rgba(0,0,0,0));\""
        "color:#d8c8ff;font:10px Consolas;letter-spacing:2px;\">"
        "🤖 JARVIS AI COPILOT <span style=\"color:#7f929f;\">ASK → VERIFY → ANSWER</span>"
        "</div>",
        unsafe_allow_html=True,
    )
    st.caption(
        "Ask a follow-up question about the current Gold data. "
        "JARVIS sends the same verified fact pack to Groq; it does not invent missing data."
    )

    quick = st.columns(3)
    quick_questions = [
        "Why did revenue change in the latest period?",
        "Which dimension should I investigate first?",
        "What additional data would validate the cause?",
    ]
    for idx, label in enumerate(quick_questions):
        if quick[idx].button(label, key=f"groq_quick_{idx}"):
            st.session_state["groq_followup_question"] = label

    question = st.text_input(
        "Ask JARVIS about this Gold data",
        value=st.session_state.get("groq_followup_question", ""),
        placeholder="Example: Why is Chennai contributing so much revenue?",
        key="groq_followup_input",
    )

    if st.button("ASK JARVIS", key="groq_followup_ask", type="secondary"):
        if not question.strip():
            st.warning("Enter a question first.")
        else:
            with st.spinner("JARVIS is reasoning over the verified Gold facts..."):
                answer = _call_groq_followup(
                    question.strip(),
                    st.session_state.get("groq_last_report", result["content"]),
                )
            st.session_state["groq_followup_result"] = answer

    followup = st.session_state.get("groq_followup_result")
    if followup:
        if followup.get("ok"):
            st.success(f"JARVIS Copilot • {followup.get('model', 'configured model')}")
            st.markdown(followup["content"])
            st.markdown("### ⚡ NEXT JARVIS ACTION")
            st.info(followup.get("next_action", "Use the verified Gold result as the starting point for the next drill-down."))
            with st.expander("Query Engine • verified retrieval details"):
                st.json({
                    "intent": followup.get("query_intent", {}),
                    "question": question.strip(),
                })
        else:
            st.error(followup.get("error", "Unknown Groq error"))

    # Downloadable plain-text copy of the current report + optional answer.
    export_text = (
        "J.A.R.V.I.S AI ANALYST REPORT\n"
        f"Model: {result.get('model', 'configured model')}\n\n"
        + result["content"]
    )
    if followup and followup.get("ok"):
        export_text += (
            "\n\n--- JARVIS COPILOT FOLLOW-UP ---\n"
            f"Question: {question.strip()}\n\n"
            + followup["content"]
        )

    st.download_button(
        "EXPORT AI REPORT",
        data=export_text,
        file_name="jarvis_ai_analyst_report.txt",
        mime="text/plain",
        key="groq_export_report",
    )

    with st.expander("Groq input — verified facts"):
        st.json(_build_groq_context())



# ============================================================
# INVESTIGATION ENGINE v6.5
# Executes the next-investigation questions against Gold data.
# ============================================================

def _investigation_analytics_df():
    path = _latest_analytics_gold()
    if path is None:
        return None
    try:
        df = pd.read_csv(path)
        return df if not df.empty else None
    except Exception:
        return None


def _investigation_detail_df():
    path = _latest_time_series()
    # Detail Gold has a different naming convention, so resolve it
    # from the latest analytics Gold target stem first.
    analytics_path = _latest_analytics_gold()
    if analytics_path is None:
        return None

    detail_path = GOLD_DIR / f"{analytics_path.stem}_detail.csv"
    if not detail_path.exists():
        return None

    try:
        df = pd.read_csv(detail_path)
        return df if not df.empty else None
    except Exception:
        return None


def _investigation_metric_column(df):
    for column in ["amount_sum", "revenue_sum", "sales_sum", "amount"]:
        if column in df.columns:
            return column
    return None


def _investigation_dimension_breakdown(df, dimension, metric):
    if dimension not in df.columns or metric not in df.columns:
        return pd.DataFrame()

    work = df[[dimension, metric]].copy()
    work[metric] = pd.to_numeric(work[metric], errors="coerce")
    work = work.dropna(subset=[metric])

    if work.empty:
        return pd.DataFrame()

    result = (
        work.groupby(dimension, dropna=False)[metric]
        .sum()
        .reset_index()
        .sort_values(metric, ascending=False)
    )

    total = result[metric].sum()
    result["share_pct"] = (
        result[metric] / total * 100 if total else 0
    )
    result["rank"] = range(1, len(result) + 1)

    return result


def _investigation_product_breakdown():
    df = _investigation_detail_df()
    if df is None or "product" not in df.columns:
        df = _investigation_analytics_df()

    if df is None or "product" not in df.columns:
        return pd.DataFrame()

    metric = "amount"
    if metric not in df.columns:
        for candidate in ["amount_sum", "revenue", "sales"]:
            if candidate in df.columns:
                metric = candidate
                break

    if metric not in df.columns:
        return pd.DataFrame()

    return _investigation_dimension_breakdown(df, "product", metric)


def render_investigation_engine():
    analytics = _investigation_analytics_df()

    st.html("""
    <div style="
        margin-top:26px;
        padding:12px 14px;
        border-left:3px solid #37e58b;
        background:linear-gradient(90deg,rgba(55,229,139,.07),rgba(0,0,0,0));
        color:#37e58b;
        font:10px Consolas;
        letter-spacing:2px;
    ">
        🔎 JARVIS INVESTIGATION ENGINE
        <span style="color:#7f929f;">DRILL-DOWN / CONTRIBUTORS / TOP / BOTTOM</span>
    </div>
    """)

    if analytics is None:
        st.info("No Analytics Gold data is available for investigation.")
        return

    metric = _investigation_metric_column(analytics)
    if metric is None:
        st.info("No supported business metric was found in Analytics Gold.")
        return

    dimensions = [
        d for d in ["category", "city", "region", "department"]
        if d in analytics.columns
    ]

    results = {}

    for dimension in dimensions:
        result = _investigation_dimension_breakdown(
            analytics, dimension, metric
        )
        if not result.empty:
            results[dimension] = result

    if results:
        cols = st.columns(min(4, len(results)))
        for i, (dimension, result) in enumerate(results.items()):
            top = result.iloc[0]
            with cols[i % len(cols)]:
                st.metric(
                    f"TOP {dimension.upper()}",
                    str(top[dimension]),
                    f"{float(top['share_pct']):.2f}% share",
                )

        for dimension, result in results.items():
            with st.expander(
                f"{dimension.upper()} CONTRIBUTION ANALYSIS",
                expanded=(dimension == "category"),
            ):
                display = result.copy()
                display[metric] = pd.to_numeric(
                    display[metric], errors="coerce"
                ).round(2)
                display["share_pct"] = display["share_pct"].round(2)

                st.dataframe(
                    display,
                    use_container_width=True,
                    hide_index=True,
                )

    product = _investigation_product_breakdown()
    if not product.empty:
        st.markdown("### PRODUCT CONTRIBUTION")
        product_display = product.copy()
        product_metric = [
            c for c in product_display.columns
            if c not in {"product", "share_pct", "rank"}
        ][0]
        product_display[product_metric] = pd.to_numeric(
            product_display[product_metric], errors="coerce"
        ).round(2)
        product_display["share_pct"] = product_display["share_pct"].round(2)

        st.dataframe(
            product_display,
            use_container_width=True,
            hide_index=True,
        )

        top_product = product.iloc[0]
        bottom_product = product.iloc[-1]

        c1, c2 = st.columns(2)
        c1.metric(
            "TOP PRODUCT",
            str(top_product["product"]),
            f"{float(top_product['share_pct']):.2f}% share",
        )
        c2.metric(
            "BOTTOM PRODUCT",
            str(bottom_product["product"]),
            f"{float(bottom_product['share_pct']):.2f}% share",
        )

    with st.expander("Investigation Engine — methodology"):
        st.markdown(
            "- Reads the latest Analytics Gold output.\n"
            "- Aggregates supported business dimensions.\n"
            "- Calculates contribution share and rank.\n"
            "- Uses Detail Gold for transaction-level product drill-down when available.\n"
            "- Does not modify source, Silver, or Gold data."
        )



def render_ai_reasoning():
    signals = _decision_signal_rows()
    contributors = _reasoning_contributors()

    st.html("""
    <div style="
        margin-top:26px;
        padding:12px 14px;
        border-left:3px solid #9b6cff;
        background:linear-gradient(90deg,rgba(155,108,255,.08),rgba(0,0,0,0));
        color:#cbb7ff;
        font:10px Consolas;
        letter-spacing:2px;
    ">
        🤖 JARVIS AI REASONING
        <span style="color:#7f929f;">WHY / CONTRIBUTION / NEXT STEP</span>
    </div>
    """)

    headline, contributor_text = _reasoning_summary(signals, contributors)

    st.markdown(f"**OBSERVATION**  \n{headline}")
    st.markdown(f"**CONTRIBUTION ANALYSIS**  \n{contributor_text}")

    if contributors:
        rows = []
        for item in contributors:
            rows.append({
                "Dimension": item["dimension"],
                "Top Value": item["value"],
                "Metric": item["metric"],
                "Contribution": f"{item['share_pct']:.2f}%",
            })

        st.dataframe(
            pd.DataFrame(rows),
            use_container_width=True,
            hide_index=True,
        )

    actions = _decision_actions(signals)

    st.markdown("### NEXT INVESTIGATION")
    if actions:
        for action in actions[:3]:
            st.markdown(f"- {action}")
    else:
        st.markdown(
            "- Inspect the latest Gold aggregation and identify the "
            "dimensions driving the current metric."
        )

    with st.expander("Reasoning inputs"):
        st.json({
            "signals": signals,
            "contributors": contributors,
            "engine": "JARVIS AI Reasoning v6.4",
            "mode": "deterministic",
        })



def render_decision_engine():
    signals = _decision_signal_rows()

    st.html("""
    <div style="
        margin-top:24px;
        padding:12px 14px;
        border-left:3px solid #ff3b3b;
        background:linear-gradient(90deg,rgba(255,59,59,.06),rgba(0,0,0,0));
        color:#ff8a8a;
        font:10px Consolas;
        letter-spacing:2px;
    ">
        🧠 JARVIS DECISION ENGINE
        <span style="color:#7f929f;">SIGNAL / IMPACT / ACTION</span>
    </div>
    """)

    if not signals:
        st.info("No decision signals detected from the available Gold outputs.")
        return

    high = sum(1 for s in signals if s["severity"] == "HIGH")
    medium = sum(1 for s in signals if s["severity"] == "MEDIUM")
    state = "ACTION REQUIRED" if high else ("REVIEW" if medium else "MONITOR")

    c1, c2, c3 = st.columns(3)
    c1.metric("SIGNALS", len(signals))
    c2.metric("HIGH PRIORITY", high)
    c3.metric("DECISION", state)

    for signal in signals:
        severity = signal["severity"]
        border = "#ff3b3b" if severity == "HIGH" else (
            "#ffd166" if severity == "MEDIUM" else "#4bd9ff"
        )

        st.html(f"""
        <div style="
            border:1px solid {border};
            border-left:4px solid {border};
            background:#03090d;
            padding:14px 16px;
            margin:10px 0;
        ">
            <div style="font-size:11px;letter-spacing:2px;">
                {html.escape(signal["severity"])} · {html.escape(signal["type"])}
            </div>
            <div style="font-size:18px;font-weight:700;margin:7px 0;">
                {html.escape(signal["title"])}
            </div>
            <div style="font-size:14px;color:#b9c7d1;">
                {html.escape(signal["message"])}
            </div>
        </div>
        """)

    actions = _decision_actions(signals)
    if actions:
        st.markdown("### RECOMMENDED ACTIONS")
        for number, action in enumerate(actions, 1):
            st.markdown(f"**{number}.** {action}")

    with st.expander("Decision Engine — signal details"):
        st.json(signals)



def render_gold_intelligence():
    """
    Gold v6.2 embedded workspace dashboard.

    Reads existing Gold v4/v5 outputs without modifying the backend:
      - Analytics Gold
      - Detail Gold
      - Time-Series Gold
    """
    analytics_path = _latest_analytics_gold()
    time_series_path = _latest_time_series()

    if analytics_path is None and time_series_path is None:
        return

    st.html("""
    <div style="
        margin-top:28px;
        padding:16px 18px;
        border:1px solid #1b3039;
        background:linear-gradient(90deg,#03080b,#061117);
    ">
        <div style="
            color:#e9f0f4;
            font:900 18px Consolas,monospace;
            letter-spacing:3px;
        ">
            GOLD INTELLIGENCE
        </div>
        <div style="
            margin-top:5px;
            color:#61737d;
            font:8px Consolas,monospace;
            letter-spacing:1.5px;
        ">
            KPI ENGINE // TIME-SERIES // GROWTH // TREND // BUSINESS SIGNALS
        </div>
    </div>
    """)

    analytics_df = None
    ts_df = None

    if analytics_path is not None:
        try:
            analytics_df = pd.read_csv(analytics_path)
        except Exception as error:
            st.warning(f"Analytics Gold read failed: {error}")

    if time_series_path is not None:
        try:
            ts_df = pd.read_csv(time_series_path)
        except Exception as error:
            st.warning(f"Time-Series Gold read failed: {error}")

    # --------------------------------------------------------
    # KPI ENGINE
    # --------------------------------------------------------
    kpi_values = {}

    if analytics_df is not None and not analytics_df.empty:
        kpi_values["ROWS"] = len(analytics_df)

        amount_sum = _metric_from_df(
            analytics_df,
            ["amount_sum", "sales_sum", "revenue_sum", "total_sum"],
        )
        quantity_sum = _metric_from_df(
            analytics_df,
            ["quantity_sum", "sales_quantity_sum"],
        )

        kpi_values["TOTAL AMOUNT"] = amount_sum
        kpi_values["TOTAL QUANTITY"] = quantity_sum

        if "_gold_quality_status" in analytics_df.columns:
            values = analytics_df["_gold_quality_status"].dropna().astype(str)
            kpi_values["QUALITY"] = (
                values.iloc[0] if not values.empty else "N/A"
            )
        else:
            kpi_values["QUALITY"] = "N/A"

        if "_reconciliation_status" in analytics_df.columns:
            values = analytics_df["_reconciliation_status"].dropna().astype(str)
            kpi_values["RECONCILIATION"] = (
                values.iloc[0] if not values.empty else "N/A"
            )
        else:
            kpi_values["RECONCILIATION"] = "N/A"

    if ts_df is not None and not ts_df.empty:
        latest = ts_df.iloc[-1]

        amount_growth = _safe_number(
            latest.get("amount_growth_pct")
        )
        amount_trend = str(
            latest.get("amount_trend", "N/A")
        )

        quantity_growth = _safe_number(
            latest.get("quantity_growth_pct")
        )
        quantity_trend = str(
            latest.get("quantity_trend", "N/A")
        )

        kpi_values["PERIOD"] = str(
            latest.get("period", "N/A")
        )
        kpi_values["AMOUNT GROWTH"] = amount_growth
        kpi_values["AMOUNT TREND"] = amount_trend
        kpi_values["QUANTITY GROWTH"] = quantity_growth
        kpi_values["QUANTITY TREND"] = quantity_trend

    cards = [
        ("ROWS", kpi_values.get("ROWS", "N/A")),
        (
            "TOTAL AMOUNT",
            _format_metric(kpi_values.get("TOTAL AMOUNT"), "money"),
        ),
        (
            "TOTAL QUANTITY",
            _format_metric(kpi_values.get("TOTAL QUANTITY")),
        ),
        ("QUALITY", kpi_values.get("QUALITY", "N/A")),
        ("LATEST PERIOD", kpi_values.get("PERIOD", "N/A")),
        (
            "AMOUNT GROWTH",
            _format_metric(kpi_values.get("AMOUNT GROWTH"), "pct"),
        ),
    ]

    cols = st.columns(6)
    for col, (label, value) in zip(cols, cards):
        with col:
            st.metric(label, value)

    # --------------------------------------------------------
    # TREND STATUS
    # --------------------------------------------------------
    if ts_df is not None and not ts_df.empty:
        st.html("""
        <div style="
            margin-top:18px;
            margin-bottom:8px;
            padding:9px 12px;
            border-left:3px solid #4bd7ff;
            background:#03080b;
            color:#b9dce8;
            font:900 10px Consolas,monospace;
            letter-spacing:2px;
        ">
            TIME-SERIES INTELLIGENCE
        </div>
        """)

        trend_a, trend_b, trend_c, trend_d = st.columns(4)

        latest = ts_df.iloc[-1]

        with trend_a:
            st.metric(
                "PERIOD",
                str(latest.get("period", "N/A")),
            )

        with trend_b:
            growth = _safe_number(latest.get("amount_growth_pct"))
            st.metric(
                "AMOUNT GROWTH",
                _format_metric(growth, "pct"),
            )

        with trend_c:
            st.metric(
                "AMOUNT TREND",
                str(latest.get("amount_trend", "N/A")),
            )

        with trend_d:
            q_growth = _safe_number(
                latest.get("quantity_growth_pct")
            )
            st.metric(
                "QUANTITY GROWTH",
                _format_metric(q_growth, "pct"),
            )

        chart_df = ts_df.copy()

        if "period" in chart_df.columns:
            chart_df["period"] = chart_df["period"].astype(str)
            chart_df = chart_df.set_index("period")

        chart_columns = [
            column
            for column in [
                "amount_sum",
                "quantity_sum",
            ]
            if column in chart_df.columns
        ]

        if chart_columns:
            chart_data = chart_df[chart_columns].apply(
                pd.to_numeric,
                errors="coerce",
            )
            st.line_chart(
                chart_data,
                use_container_width=True,
            )

        with st.expander("VIEW TIME-SERIES DATA"):
            st.dataframe(
                ts_df,
                use_container_width=True,
                hide_index=True,
            )

    # --------------------------------------------------------
    # BUSINESS SIGNALS
    # --------------------------------------------------------
    if analytics_df is not None and not analytics_df.empty:
        st.html("""
        <div style="
            margin-top:18px;
            margin-bottom:8px;
            padding:9px 12px;
            border-left:3px solid #37e58b;
            background:#03080b;
            color:#b9dce8;
            font:900 10px Consolas,monospace;
            letter-spacing:2px;
        ">
            BUSINESS SIGNALS
        </div>
        """)

        signals = []

        if "amount_sum" in analytics_df.columns:
            amount_numeric = pd.to_numeric(
                analytics_df["amount_sum"],
                errors="coerce",
            )
            if amount_numeric.notna().any():
                idx = amount_numeric.idxmax()
                row = analytics_df.loc[idx]

                dimensions = []
                for column in ["category", "city", "region", "department"]:
                    if column in analytics_df.columns:
                        dimensions.append(
                            f"{column}={row[column]}"
                        )

                if dimensions:
                    signals.append(
                        "Highest amount segment: "
                        + " | ".join(dimensions)
                        + f" | amount_sum={amount_numeric.loc[idx]:,.2f}"
                    )

        if "amount_performance" in analytics_df.columns:
            values = analytics_df["amount_performance"].dropna()
            if not values.empty:
                signals.append(
                    f"Amount performance labels detected: "
                    f"{values.astype(str).value_counts().to_dict()}"
                )

        if ts_df is not None and not ts_df.empty:
            if "amount_trend" in ts_df.columns:
                latest_trend = str(
                    ts_df.iloc[-1]["amount_trend"]
                )
                signals.append(
                    f"Latest amount trend: {latest_trend}"
                )

            if "amount_growth_pct" in ts_df.columns:
                latest_growth = _safe_number(
                    ts_df.iloc[-1]["amount_growth_pct"]
                )
                if latest_growth is not None:
                    signals.append(
                        f"Latest amount growth: {latest_growth:,.2f}%"
                    )

        if not signals:
            signals.append(
                "Gold analytics available. No additional business signal columns detected."
            )

        for signal in signals:
            st.info(signal)

    # --------------------------------------------------------
    # SOURCE FILES
    # --------------------------------------------------------
    source_items = []

    if analytics_path is not None:
        source_items.append(
            f"Analytics: `{rel(analytics_path)}`"
        )

    if time_series_path is not None:
        source_items.append(
            f"Time-Series: `{rel(time_series_path)}`"
        )

    if source_items:
        st.caption(" • ".join(source_items))

def render_files():
    st.html(
        '<div style="margin-top:25px;color:#65737d;font:8px Consolas;letter-spacing:3px;">'
        'DATA ENGINEERING FILE SYSTEM'
        '</div>'
    )

    search = st.text_input(
        "FILE SEARCH",
        placeholder="Search files...",
        label_visibility="collapsed",
    )

    files = all_files()

    if search.strip():
        q = search.lower()
        files = [
            p for p in files
            if q in p.name.lower() or q in rel(p).lower()
        ]

    tabs = st.tabs(["RAW", "BRONZE", "SILVER", "GOLD", "METADATA"])

    for tab, layer in zip(
        tabs,
        ["RAW", "BRONZE", "SILVER", "GOLD", "METADATA"],
    ):
        with tab:

            if layer == "GOLD":
                analytics, detail, time_series = gold_files()

                if search.strip():
                    q = search.lower()
                    analytics = [p for p in analytics if q in p.name.lower()]
                    detail = [p for p in detail if q in p.name.lower()]
                    time_series = [p for p in time_series if q in p.name.lower()]

                total = len(analytics) + len(detail) + len(time_series)

                st.html(f"""
                <div class="gold-layer-header" style="
                    border:1px solid #163640;
                    background:linear-gradient(90deg,#02080b,#061117);
                    padding:18px 22px;
                ">
                    <div>
                        <div class="gold-layer-title">GOLD LAYER</div>
                        <div class="gold-layer-subtitle">
                            Business-ready data products
                        </div>
                    </div>
                    <div class="gold-layer-counts">
                        <span>📊 ANALYTICS <b>{len(analytics)}</b></span>
                        <span>📋 DETAIL <b>{len(detail)}</b></span>
                        <span>📈 TIME SERIES <b>{len(time_series)}</b></span>
                        <span>TOTAL <b>{total}</b></span>
                    </div>
                </div>
                """)

                # Gold Intelligence belongs to the Gold workspace.
                # It is intentionally rendered inside the GOLD tab instead of
                # appearing as a separate page-level dashboard.
                if analytics or time_series:
                    render_gold_intelligence()
                    render_decision_engine()
                    render_ai_reasoning()
                    render_investigation_engine()
                    render_groq_analyst()

                    st.html("""
                    <div style="
                        margin-top:24px;
                        margin-bottom:12px;
                        padding:10px 14px;
                        border-left:3px solid #37e58b;
                        background:linear-gradient(
                            90deg,
                            rgba(55,229,139,.05),
                            rgba(0,0,0,0)
                        );
                        color:#37e58b;
                        font:10px Consolas;
                        letter-spacing:2px;
                    ">
                        GOLD DATA PRODUCTS
                        <span style="
                            float:right;
                            color:#596870;
                            font-size:8px;
                            letter-spacing:1px;
                        ">
                            ANALYTICS / DETAIL / TIME SERIES
                        </span>
                    </div>
                    """)

                st.html("""
                <div class="gold-section-label">
                    📊 ANALYTICS GOLD
                    <span>BUSINESS AGGREGATION / KPI</span>
                </div>
                """)

                if analytics:
                    for path in analytics:
                        render_gold_card(path, "ANALYTICS")
                else:
                    st.caption("No Analytics Gold CSV found.")

                st.html("""
                <div class="gold-section-label detail-label">
                    📋 DETAIL GOLD
                    <span>TRANSACTION LEVEL</span>
                </div>
                """)

                if detail:
                    for path in detail:
                        render_gold_card(path, "DETAIL")
                else:
                    st.caption("No Detail Gold CSV found.")

                st.html("""
                <div class="gold-section-label" style="
                    margin-top:26px;
                    border-left-color:#4bd9ff;
                ">
                    📈 TIME SERIES GOLD
                    <span>TREND / GROWTH / PERIOD INTELLIGENCE</span>
                </div>
                """)

                if time_series:
                    for path in time_series:
                        render_gold_card(path, "TIME_SERIES")
                else:
                    st.caption(
                        "No Time-Series Gold CSV found. "
                        "A date/timestamp column is required."
                    )

                continue

            layer_files = [p for p in files if layer_of(p) == layer]

            if not layer_files:
                st.caption(f"No {layer} files.")
                continue

            st.html("""
            <div class="file-head">
                <div>FILE</div>
                <div>LAYER</div>
                <div>PATH</div>
            </div>
            """)

            for index, path in enumerate(layer_files):
                c1, c2, c3, c4 = st.columns([3, 1, 4, 1])

                with c1:
                    st.write(f"▣ {path.name}")

                with c2:
                    st.caption(layer)

                with c3:
                    st.caption(rel(path))

                with c4:
                    if st.button(
                        "VIEW",
                        key=f"view_{layer}_{index}_{path.name}",
                    ):
                        st.session_state.selected_file = str(path)
                        st.rerun()

    if not st.session_state.selected_file:
        return

    selected = Path(st.session_state.selected_file)

    if not selected.exists():
        return

    st.html(
        '<div style="margin-top:20px;color:#65737d;font:8px Consolas;letter-spacing:3px;">'
        'FILE INSPECTOR'
        '</div>'
    )

    if layer_of(selected) == "GOLD":
        kind = gold_type(selected)
        if kind == "DETAIL":
            st.html('<div class="gold-inspector detail">📋 DETAIL GOLD // TRANSACTION LEVEL</div>')
        elif kind == "ANALYTICS":
            st.html('<div class="gold-inspector analytics">📊 ANALYTICS GOLD // BUSINESS AGGREGATION</div>')

    st.caption(rel(selected))

    if selected.suffix.lower() == ".csv":
        try:
            # RAW layer: show the complete dataset.
            # Other layers keep the existing 500-row preview
            # so large Gold/Silver files remain responsive.
            if layer_of(selected) == "RAW":
                df = pd.read_csv(selected)
                row_label = "ROWS"
            else:
                df = pd.read_csv(selected, nrows=500)
                row_label = "PREVIEW ROWS"

            a, b, c, d = st.columns(4)
            a.metric(row_label, len(df))
            b.metric("COLUMNS", len(df.columns))
            c.metric("LAYER", layer_of(selected))

            if layer_of(selected) == "GOLD":
                d.metric("GOLD TYPE", gold_type(selected))
            else:
                d.metric("SIZE", f"{selected.stat().st_size / (1024 * 1024):.2f} MB")

            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
            )

        except Exception as error:
            st.error(str(error))

    elif selected.suffix.lower() == ".json":
        data = read_json(selected)
        payload = data.get("data", data) if isinstance(data, dict) else data
        category = category_of(selected)

        if (
            category in {"schema", "semantic"}
            and isinstance(payload, dict)
            and payload.get("columns")
        ):
            st.dataframe(
                pd.DataFrame(payload["columns"]),
                use_container_width=True,
                hide_index=True,
            )
        elif selected.name.endswith("_gold.json"):
            rows = payload.get("data", []) if isinstance(payload, dict) else []
            if rows:
                st.dataframe(
                    pd.DataFrame(rows),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.json(payload)
        elif isinstance(payload, dict):
            st.dataframe(
                pd.DataFrame(
                    [{"FIELD": key, "VALUE": str(value)} for key, value in payload.items()]
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.json(payload)

        with st.expander("VIEW RAW JSON"):
            st.json(data)

    else:
        try:
            st.code(selected.read_text(encoding="utf-8"), language="text")
        except Exception as error:
            st.error(str(error))

def render_history():
    st.html('<div style="margin-top:25px;color:#65737d;font:8px Consolas;letter-spacing:3px;">EXECUTION HISTORY</div>')

    history = read_history()

    if not history:
        st.caption("No JARVIS execution history yet.")
        return

    success = sum(1 for x in history if x.get("status") == "SUCCESS")
    failed = sum(1 for x in history if x.get("status") == "FAILED")

    a, b, c = st.columns(3)
    a.metric("TOTAL RUNS", len(history))
    b.metric("SUCCESS", success)
    c.metric("FAILED", failed)

    for record in reversed(history):
        icon = "🟢" if record.get("status") == "SUCCESS" else "🔴"

        with st.expander(
            f"{icon} {record.get('run_id', 'RUN')} — {record.get('command', '')}"
        ):
            a, b, c, d = st.columns(4)
            a.metric("STATUS", record.get("status", "UNKNOWN"))
            b.metric("TARGET", record.get("target", "N/A"))
            c.metric("RETURN", record.get("return_code", "N/A"))
            d.metric("RUN ID", record.get("run_id", "N/A"))

            st.caption(record.get("timestamp", ""))

            if record.get("silver_path"):
                st.write("Silver:", record["silver_path"])

            if record.get("gold_path"):
                st.write("Gold:", record["gold_path"])

            with st.expander("VIEW EXECUTION LOG"):
                st.code(
                    record.get("execution_log", ""),
                    language="text",
                )


# ============================================================
# TACTICAL HUD
# ============================================================

def render_tactical():

    status = st.session_state.last_status

    if status == "SUCCESS":
        voice_state = "● SYSTEM READY"
        voice_color = "#37e58b"
    elif status == "FAILED":
        voice_state = "● SYSTEM ERROR"
        voice_color = "#e3293b"
    elif status == "RUNNING":
        voice_state = "● PROCESSING"
        voice_color = "#f5c84c"
    else:
        voice_state = "● VOICE COMMAND READY"
        voice_color = "#37e58b"

    command = html.escape(
        st.session_state.last_command
        or "Awaiting voice command..."
    )

    st.html(
        f"""
        <div class="tactical">

            <div class="corner tl"></div>
            <div class="corner tr"></div>
            <div class="corner bl"></div>
            <div class="corner br"></div>

            <div class="topbar">

                <div class="brand">
                    J.A.R.V.I.S <span>// TACTICAL CORE</span>
                </div>

                <div class="top-stat">
                    STATUS <b>{status}</b>
                    &nbsp;&nbsp;
                    RAW <b>{count_files(RAW_DIR)}</b>
                    &nbsp;&nbsp;
                    SILVER <b>{count_files(SILVER_DIR)}</b>
                    &nbsp;&nbsp;
                    GOLD <b>{count_files(GOLD_DIR)}</b>
                    &nbsp;&nbsp;
                    CORE <b>v{VERSION}</b>
                </div>

            </div>

            <div class="hud-layout">

                <div class="hud-side left">

                    <div class="side-title">SYSTEM TELEMETRY</div>

                    <div class="side-line">
                        <span>CPU</span>
                        <b class="cyan">ONLINE</b>
                    </div>

                    <div class="side-line">
                        <span>MEMORY</span>
                        <b>ACTIVE</b>
                    </div>

                    <div class="side-line">
                        <span>AI BRAIN</span>
                        <b class="green">GROQ</b>
                    </div>

                    <div class="side-line">
                        <span>COMMAND</span>
                        <b class="cyan">READY</b>
                    </div>

                    <div class="side-line">
                        <span>ORCHESTRATOR</span>
                        <b class="green">ONLINE</b>
                    </div>

                    <div class="side-line">
                        <span>ARTIFACTS</span>
                        <b>{len(metadata_files())}</b>
                    </div>

                    <div class="side-line">
                        <span>RUNS</span>
                        <b>{len(read_history())}</b>
                    </div>

                    <div style="margin-top:22px;color:#3c4c55;font:7px Consolas;line-height:1.7;">
                        VOICE INTERFACE<br>
                        TEXT COMMANDS<br>
                        WORKFLOW PLANNER<br>
                        DATA QUALITY<br>
                        SILVER / GOLD
                    </div>

                </div>

                <div class="core-zone">

                    <div class="core-grid"></div>

                    <div class="voice-state" style="color:{voice_color};">
                        {voice_state}
                    </div>

                    <div class="robot">

                        <div class="helmet">
                            <div class="eye left"></div>
                            <div class="eye right"></div>
                        </div>

                        <div class="jaw"></div>
                        <div class="mouth"></div>

                        <div class="shoulders"></div>

                        <div class="chest"></div>

                        <div class="reactor"></div>

                    </div>

                    <div class="core-label">
                        J.A.R.V.I.S // AI DATA CORE
                    </div>

                </div>

                <div class="hud-side right">

                    <div class="side-title">SYSTEM RADAR</div>

                    <div class="radar">
                        <div class="radar-dot one"></div>
                        <div class="radar-dot two"></div>
                        <div class="radar-dot three"></div>
                    </div>

                    <div class="side-title">PIPELINE STATE</div>

                    <div class="side-line">
                        <span>RAW</span>
                        <b>{count_files(RAW_DIR)}</b>
                    </div>

                    <div class="side-line">
                        <span>BRONZE</span>
                        <b>{count_files(BRONZE_DIR)}</b>
                    </div>

                    <div class="side-line">
                        <span>SILVER</span>
                        <b class="cyan">{count_files(SILVER_DIR)}</b>
                    </div>

                    <div class="side-line">
                        <span>GOLD</span>
                        <b class="green">{count_files(GOLD_DIR)}</b>
                    </div>

                </div>

            </div>

        </div>
        """
    )

    st.html(
        """
        <div style="
            margin-top:18px;
            color:#65737d;
            font:8px Consolas;
            letter-spacing:3px;
        ">
            COMMAND CENTER
        </div>
        """
    )

    st.html(
        f"""
        <div class="command-panel">
            <div class="command-label">JARVIS://VOICE_AND_TEXT_COMMAND</div>
            <div style="
                color:#6d7c85;
                font:9px Consolas;
            ">
                &gt; {command}
            </div>
        </div>
        """
    )

    user_command = st.text_input(
        "COMMAND",
        value=st.session_state.last_command,
        placeholder="employees.csv full pipeline pannu",
        label_visibility="collapsed",
        key="tactical_command",
    )

    a, b = st.columns([6, 1])

    with a:
        execute = st.button(
            "◉  EXECUTE J.A.R.V.I.S",
            use_container_width=True,
            key="tactical_execute",
        )

    with b:
        clear = st.button(
            "CLEAR",
            use_container_width=True,
            key="tactical_clear",
        )

    if clear:
        st.session_state.last_command = ""
        st.session_state.last_output = ""
        st.session_state.last_status = "IDLE"
        st.session_state.selected_file = None
        st.session_state.orchestrator_report = None
        st.rerun()

    if execute:
        pipeline_placeholder = st.empty()
        live = st.empty()
        run_backend(
            user_command,
            live,
            pipeline_placeholder,
        )

    render_pipeline()
    render_events()
    render_orchestrator_monitor()
    render_files()
    render_history()


# ============================================================
# CLASSIC HUD
# ============================================================

def render_classic():

    st.html(
        f"""
        <div class="classic-header">
            <div class="classic-title">J.A.R.V.I.S</div>
            <div style="color:#37e58b;font:9px Consolas;letter-spacing:2px;">
                ● SYSTEM ONLINE<br>
                <span style="color:#596870;">CLASSIC CORE v{VERSION}</span>
            </div>
            <div class="classic-subtitle">
                ARTIFICIAL INTELLIGENCE DATA ENGINEERING SYSTEM
            </div>
        </div>
        """
    )

    left, right = st.columns([2, 6])

    with left:
        st.html(
            """
            <div class="classic-core">
                <div class="classic-orb">
                    <div style="
                        text-align:center;
                        font-weight:900;
                        letter-spacing:4px;
                    ">
                        JARVIS
                        <div style="
                            color:#37e58b;
                            font:8px Consolas;
                            margin-top:7px;
                        ">
                            ● ONLINE
                        </div>
                    </div>
                </div>
            </div>
            """
        )

    with right:
        cards = [
            ("SYSTEM", st.session_state.last_status),
            ("RAW", count_files(RAW_DIR)),
            ("SILVER", count_files(SILVER_DIR)),
            ("GOLD", count_files(GOLD_DIR)),
            ("METADATA", len(metadata_files())),
            ("RUNS", len(read_history())),
        ]

        for row in [cards[:3], cards[3:]]:
            cols = st.columns(3)
            for col, (label, value) in zip(cols, row):
                with col:
                    st.metric(label, value)

    st.html(
        '<div style="margin-top:18px;color:#65737d;font:8px Consolas;letter-spacing:3px;">COMMAND CENTER</div>'
    )

    command = st.text_input(
        "CLASSIC COMMAND",
        value=st.session_state.last_command,
        placeholder="employees.csv full pipeline pannu",
        label_visibility="collapsed",
        key="classic_command",
    )

    a, b = st.columns([6, 1])

    with a:
        execute = st.button(
            "EXECUTE JARVIS",
            use_container_width=True,
            key="classic_execute",
        )

    with b:
        clear = st.button(
            "CLEAR",
            use_container_width=True,
            key="classic_clear",
        )

    if clear:
        st.session_state.last_command = ""
        st.session_state.last_output = ""
        st.session_state.last_status = "IDLE"
        st.session_state.selected_file = None
        st.session_state.orchestrator_report = None
        st.rerun()

    if execute:
        pipeline_placeholder = st.empty()
        live = st.empty()
        run_backend(
            command,
            live,
            pipeline_placeholder,
        )

    render_pipeline()
    render_events()
    render_orchestrator_monitor()
    render_files()
    render_history()


# ============================================================
# SIDEBAR FUNCTIONAL PANELS
# ============================================================

def render_sidebar_panel():
    view = st.session_state.sidebar_view

    if view == "OVERVIEW":
        return False

    st.html(f"""
    <div style="margin-top:18px;padding:16px 18px;border:1px solid #1d3540;
                background:linear-gradient(90deg,rgba(8,20,26,.95),rgba(0,0,0,.95));">
        <div style="color:#53d9ff;font:11px Consolas;letter-spacing:3px;">J.A.R.V.I.S // {html.escape(view)}</div>
        <div style="color:#65737d;font:9px Consolas;margin-top:6px;">SIDEBAR CONTROL PANEL</div>
    </div>
    """)

    if view in {"COMMAND", "WORKFLOW", "RUN"}:
        st.info("Use the command center below to send a pipeline command to JARVIS.")
        if st.session_state.ui_mode == "TACTICAL HUD":
            render_tactical()
        else:
            render_classic()
        return True

    if view == "ORCHESTRATOR":
        render_orchestrator_monitor()
        render_pipeline()
        render_events()
        return True

    if view == "ARTIFACTS":
        render_files()
        return True

    if view == "HISTORY":
        render_history()
        return True

    if view == "LIVE":
        render_events()
        render_orchestrator_monitor()
        return True

    if view in {"ERRORS", "RETRY"}:
        report = st.session_state.get("orchestrator_report") or latest_orchestrator_report()
        if not report:
            st.warning("No orchestrator execution report found yet.")
            return True
        errors = report.get("errors", []) or []
        steps = report.get("steps", []) or []
        retries = report.get("retries", 0)
        st.metric("TOTAL RETRIES", retries)
        if errors:
            st.error(f"{len(errors)} error event(s) captured.")
            st.json(errors)
        else:
            st.success("No errors recorded in the latest execution.")
        if steps:
            st.dataframe(pd.DataFrame(steps), use_container_width=True, hide_index=True)
        return True

    if view == "METRICS":
        report = st.session_state.get("orchestrator_report") or latest_orchestrator_report()
        if not report:
            st.warning("No execution metrics available yet.")
            return True
        steps = report.get("steps", []) or []
        a,b,c,d = st.columns(4)
        a.metric("STATUS", report.get("status", "UNKNOWN"))
        b.metric("DURATION", f"{float(report.get('duration_seconds',0)):.3f}s")
        c.metric("RETRIES", report.get("retries", 0))
        d.metric("ARTIFACTS", len(report.get("artifacts", []) or []))
        if steps:
            st.dataframe(pd.DataFrame(steps), use_container_width=True, hide_index=True)
        return True

    if view == "CHECKPOINTS":
        checkpoint_dir = METADATA_DIR / "checkpoints"
        checkpoint_files = sorted(checkpoint_dir.glob("*.json")) if checkpoint_dir.exists() else []
        if not checkpoint_files:
            st.info("No checkpoints found yet. Orchestrator v7.2 will create them after incremental processing is enabled.")
        else:
            for cp in checkpoint_files:
                with st.expander(cp.name, expanded=False):
                    st.json(read_json(cp))
        return True

    if view == "INCREMENTAL":
        checkpoint_dir = METADATA_DIR / "checkpoints"
        checkpoint_files = sorted(checkpoint_dir.glob("*.json")) if checkpoint_dir.exists() else []
        st.info("Incremental processing is controlled by the Orchestrator. Checkpoint files are shown here when available.")
        if checkpoint_files:
            for cp in checkpoint_files:
                data = read_json(cp)
                st.write(f"**{cp.stem}** — {data.get('status','UNKNOWN')} — processed rows: {data.get('processed_row_count','N/A')}")
        else:
            st.warning("No incremental checkpoint exists yet.")
        return True

    if view in {"RAW", "BRONZE", "SILVER", "GOLD", "METADATA"}:
        render_files()
        return True

    if view == "LINEAGE":
        st.markdown("### DATA LINEAGE")
        st.code("RAW → BRONZE → SILVER → GOLD → ANALYTICS", language="text")
        target = extract_target(st.session_state.last_command or "")
        if target:
            st.write(f"Current target: `{target}`")
        st.write("Metadata and execution reports are stored under `data/metadata`." )
        return True

    if view in {"AZURE", "DATABRICKS", "FABRIC", "AWS"}:
        st.info(f"{view} connector panel is reserved for the platform integration layer.")
        st.caption("The sidebar navigation is active; connector execution will be wired when the corresponding SDK/API adapter is enabled.")
        return True

    if view == "BRAIN":
        st.markdown("### JARVIS BRAIN")
        st.write("Command Agent → Workflow Planner → Orchestrator")
        st.write("AI provider: Groq")
        return True

    if view == "GROQ":
        render_groq_analyst()
        return True

    if view == "COPILOT":
        render_groq_analyst()
        return True

    return False


# ============================================================
# RAW FILE UPLOAD
# ============================================================

def render_raw_upload():
    st.html("""
    <div style="
        margin-top:18px;
        padding:18px;
        border:1px solid #1d2b33;
        background:linear-gradient(135deg,rgba(8,14,18,.96),rgba(5,9,12,.96));
        box-shadow:0 0 24px rgba(0,180,255,.05);
    ">
        <div style="color:#9aaab3;font:9px Consolas,monospace;letter-spacing:3px;">
            RAW DATA INGESTION
        </div>
        <div style="color:#dce8ee;font:20px Consolas,monospace;margin-top:8px;">
            📤 UPLOAD NEW RAW FILE
        </div>
        <div style="color:#60717b;font:10px Consolas,monospace;margin-top:6px;">
            FRONTEND → BACKEND STORAGE → JARVIS AI PIPELINE
        </div>
    </div>
    """)

    uploaded = st.file_uploader(
        "UPLOAD RAW CSV",
        type=["csv"],
        accept_multiple_files=False,
        key="raw_csv_uploader",
        label_visibility="collapsed",
    )

    if uploaded is None:
        return

    safe_name = Path(uploaded.name).name

    st.markdown(
        f"**READY:** `{safe_name}`  ·  "
        f"`{uploaded.size:,} bytes`"
    )

    ingest = st.button(
        "⬆ STORE RAW FILE & START JARVIS",
        type="primary",
        use_container_width=True,
        key="ingest_raw_button",
    )

    if not ingest:
        return

    if not safe_name.lower().endswith(".csv"):
        st.error("Only CSV files are allowed.")
        return

    output_path = RAW_DIR / safe_name

    try:
        # 1. Store the uploaded CSV in the backend RAW layer.
        written_bytes = stream_uploaded_file(uploaded, output_path)

        # 2. Store ingestion metadata.
        metadata_path = save_ingestion_record(
            output_path,
            written_bytes,
        )

        # 3. Trigger the existing JARVIS backend.
        command = f"{safe_name} full pipeline pannu"
        st.session_state.last_command = command
        st.session_state.last_status = "RUNNING"

        st.success(
            f"RAW FILE STORED → `{rel(output_path)}`"
        )
        st.info(
            f"Backend metadata → `{rel(metadata_path)}`"
        )

        st.html("""
        <div style="
            margin:12px 0;
            padding:14px;
            border:1px solid #1d3540;
            background:#071116;
            color:#69d7ff;
            font:11px Consolas,monospace;
            line-height:1.8;
        ">
            ◉ RAW FILE STORED<br>
            ↓<br>
            ◉ INGESTION METADATA STORED<br>
            ↓<br>
            🧠 JARVIS BACKEND STARTING<br>
            ↓<br>
            ◉ COMMAND AGENT<br>
            ↓<br>
            ◉ WORKFLOW PLANNER<br>
            ↓<br>
            ◉ ORCHESTRATOR
        </div>
        """)

        pipeline_placeholder = st.empty()
        live = st.empty()

        run_backend(
            command,
            live,
            pipeline_placeholder,
        )

    except Exception as error:
        st.session_state.last_status = "FAILED"
        st.error(
            f"RAW FILE INGESTION FAILED: {error}"
        )


# ============================================================
# MAIN
# ============================================================

render_raw_upload()

if not render_sidebar_panel():
    if st.session_state.ui_mode == "TACTICAL HUD":
        render_tactical()
    else:
        render_classic()

st.html(
    f"""
    <div class="footer">
        J.A.R.V.I.S v{VERSION}
        &nbsp;•&nbsp;
        COMMAND → PLAN → ORCHESTRATE → VALIDATE → SILVER → GOLD
    </div>
    """
)
