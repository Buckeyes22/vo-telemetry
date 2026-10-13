"""Regenerate portable, code-derived documentation SVGs with no external assets."""

from html import escape
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "docs" / "images"


def canvas(title, subtitle, height):
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="1120" height="{height}" viewBox="0 0 1120 {height}" role="img" aria-label="{escape(title)}">',
        f"<title>{escape(title)}</title><desc>{escape(subtitle)}</desc>",
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#597087"/></marker></defs>',
        "<style>text{font-family:Arial,Helvetica,sans-serif}.h{font-size:27px;font-weight:700;fill:#142b44}.sub{font-size:16px;fill:#52677b}.label{font-size:18px;font-weight:700;fill:#173451}.body{font-size:15px;fill:#405c73}.cap{font-size:13px;letter-spacing:1px;font-weight:700;fill:#31706e}</style>",
        f'<rect width="1120" height="{height}" rx="16" fill="#f5f8fb"/>',
        f'<text x="36" y="48" class="h">{escape(title)}</text>',
        f'<text x="36" y="76" class="sub">{escape(subtitle)}</text>',
    ]


def box(parts, x, y, w, h, title, lines, tint="#ffffff"):
    parts += [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{tint}" stroke="#cedbe6"/>',
        f'<text x="{x + 18}" y="{y + 32}" class="label">{escape(title)}</text>',
    ]
    for i, line in enumerate(lines):
        parts.append(f'<text x="{x + 18}" y="{y + 60 + i * 22}" class="body">{escape(line)}</text>')


def arrow(parts, x1, y1, x2, y2):
    parts.append(
        f'<path d="M{x1},{y1} L{x2},{y2}" fill="none" stroke="#597087" stroke-width="2" marker-end="url(#arrow)"/>'
    )


def architecture():
    p = canvas(
        "Two planes, one operational view", "Implemented lifecycle path above; illustrative OTel trace path below.", 642
    )
    p.append('<text x="36" y="115" class="cap">LIVENESS · FACTS AND STATE</text>')
    box(
        p,
        36,
        132,
        250,
        125,
        "Lifecycle producers",
        ["Hooks and supervised CLIs", "Transcript reconciliation", "run / lane / tool events"],
    )
    box(
        p,
        326,
        132,
        290,
        125,
        "Durable local delivery",
        ["SQLite outbox → FIFO forwarder", "Retry + priority shedding", "Explicit loss records"],
    )
    box(
        p,
        656,
        132,
        428,
        125,
        "Liveness backend",
        [
            "Bearer-authenticated schema validation",
            "SQLite WAL → reducer + checkpoint",
            "Deduplication, replay, terminal protection",
        ],
    )
    arrow(p, 286, 192, 326, 192)
    arrow(p, 616, 192, 656, 192)
    box(
        p,
        656,
        300,
        428,
        122,
        "Read-only consumers",
        [
            "Textual dashboard + governed watcher",
            "Snapshots, event pages, /readyz, /metrics",
            "Dry-run notification by default",
        ],
        "#eaf5f2",
    )
    arrow(p, 870, 257, 870, 300)
    box(
        p,
        36,
        300,
        580,
        122,
        "Correlation stays separate",
        [
            "Liveness: run_id / agent_id / tool_run_id",
            "Distributed traces: TRACEPARENT / trace_id",
            "A heartbeat is not proof of useful progress.",
        ],
        "#edf2f9",
    )
    p.append('<text x="36" y="459" class="cap">OBSERVABILITY · REQUEST CORRELATION</text>')
    box(p, 36, 476, 250, 98, "CLI OTel context", ["Adapter exporter environment"])
    box(p, 326, 476, 290, 98, "Edge → gateway collector", ["Different sockets + queue dirs"])
    box(p, 656, 476, 428, 98, "Local debug trace export", ["Template collector config for local debugging"])
    arrow(p, 286, 523, 326, 523)
    arrow(p, 616, 523, 656, 523)
    p.append('<text x="36" y="613" class="sub">Local single-node implementation.</text></svg>')
    return "\n".join(p)


def delivery():
    p = canvas(
        "From producer fact to replayable state",
        "Delivery acknowledgement and reducer progress are distinct observations.",
        624,
    )
    heads = [("Producer", 60), ("Outbox / forwarder", 328), ("HTTP ingest", 600), ("WAL / reducer", 870)]
    for title, x in heads:
        p.append(f'<text x="{x}" y="122" class="label">{escape(title)}</text>')
        p.append(f'<path d="M{x + 66},141 V548" stroke="#c3d1de" stroke-width="2" stroke-dasharray="6 5"/>')
    messages = [
        (126, 394, 184, "1. Append local fact"),
        (394, 666, 247, "2. FIFO send + bearer token"),
        (666, 936, 310, "3. Validate → durable append"),
        (936, 666, 373, "4. New event or duplicate ack"),
        (666, 394, 436, "5. Mark sent; failure → retry"),
    ]
    for a, b, y, label in messages:
        arrow(p, a, y, b, y)
        p.append(f'<text x="{min(a, b) + 8}" y="{y - 12}" class="body">{escape(label)}</text>')
    box(p, 766, 463, 320, 84, "Reduce + checkpoint", ["Serve snapshots and metrics"], "#eaf5f2")
    p.append(
        '<text x="36" y="587" class="sub">Lost acknowledgement → retransmit same event_id → dedupe. Local storage failure remains a real limit.</text></svg>'
    )
    return "\n".join(p)


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    for name, body in [("architecture.svg", architecture()), ("delivery-sequence.svg", delivery())]:
        (OUT / name).write_text(body)
        print("Generated", name)
