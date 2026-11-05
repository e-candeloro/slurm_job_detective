"""Build the HyperFrames composition for the README demo GIF.

Runs the real `sjdet` CLI against the data in `scripts/mock_cli.py` and writes
the HyperFrames project in `media/demo/hyperframes/`: a terminal that types
`sjdet`, prints the table, then spotlights the main columns. Render it with
the HyperFrames CLI:

    uv run python scripts/build_demo_video.py
    cd media/demo/hyperframes
    npx hyperframes render --format gif --fps 15 -o ../sjdet_demo.gif
"""

from __future__ import annotations

import html
import importlib.util
import io
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple
from unittest.mock import patch

from rich.console import Console
from rich.terminal_theme import TerminalTheme
from rich.text import Text

ROOT = Path(__file__).resolve().parent.parent
PROJECT = ROOT / "media" / "demo" / "hyperframes"
SCENE_ID = "demo"
TERM_COLS = 172
FONT_PX = 17
LINE_PX = 21
DURATION = 19.5

THEME = TerminalTheme(
    (13, 17, 23),
    (230, 237, 243),
    [
        (72, 79, 88), (255, 123, 114), (63, 185, 80), (210, 153, 34),
        (88, 166, 255), (188, 140, 255), (57, 197, 207), (177, 186, 196),
    ],
    [
        (110, 118, 129), (255, 161, 152), (86, 211, 100), (227, 179, 65),
        (121, 192, 255), (210, 168, 255), (86, 212, 221), (240, 246, 252),
    ],
)

# (id, header labels to spotlight, caption, start, end)
SPOTLIGHTS = [
    ("cpu", ["Cpu Eff %"], "CPU time &divide; (elapsed &times; allocated CPUs)", 4.4, 7.2),
    ("ram", ["RAM Max Util %"], "Peak RAM over all tasks vs. allocation", 7.4, 10.4),
    ("gpu", ["GPU VRAM %"], "VRAM across all allocated GPUs", 10.6, 13.2),
    ("io", ["Page Faults", "I/O Write", "I/O Read"], "I/O rates between fresh <b>sstat</b> samples", 13.4, 16.2),
]
TAGLINE = "One <b>squeue</b> + one <b>sstat</b> per run &middot; cached"
COMMAND = "sjdet"
TYPE_START = 1.35
OUTPUT_START = 2.75
LINE_STAGGER = 0.032


def _load_mock():
    spec = importlib.util.spec_from_file_location("mock_cli", ROOT / "scripts" / "mock_cli.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def capture_cli_output() -> List[Text]:
    """Run `sjdet` against the mock data and return its output lines."""
    mock = _load_mock()
    from sjdet import cli

    console = Console(
        width=TERM_COLS, force_terminal=True, color_system="truecolor", file=io.StringIO()
    )
    os.environ["USER"] = mock.MOCK_USER
    mock._seed_mock_cache()
    try:
        with (
            patch("sjdet.cli.run", side_effect=mock.fake_run),
            patch("sjdet.slurm.run", side_effect=mock.fake_run),
            patch("sjdet.cli.maybe_update_notice", return_value=(None, {})),
            patch.object(cli, "console", console),
            patch.object(sys, "argv", ["sjdet", "--force-update-nodes"]),
            console.capture() as captured,
        ):
            cli.main()
    finally:
        mock._cleanup_mock_cache()

    lines = [Text.from_ansi(line) for line in captured.get().splitlines()]
    # Drop only empty lines: blank-looking ones may carry a row background.
    while lines and not lines[-1].plain:
        lines.pop()
    while lines and not lines[0].plain:
        lines.pop(0)
    return lines


def line_html(text: Text, console: Console) -> str:
    parts = []
    for seg in text.render(console):
        content = html.escape(seg.text, quote=False)
        css = seg.style.get_html_style(THEME) if seg.style else ""
        parts.append(f'<span style="{css}">{content}</span>' if css else content)
    return "".join(parts) or " "


def column_spans(header: str) -> Dict[str, Tuple[int, int]]:
    """Approximate [start, end) character spans of each centered column label."""
    labels = [
        "JobID", "Name", "State", "Elapsed", "Node", "Cpu Eff %", "RAM Max Util %",
        "GPU VRAM %", "Page Faults", "I/O Write", "I/O Read",
    ]
    centers = []
    for label in labels:
        pos = header.find(label)
        if pos >= 0:
            centers.append((label, pos + len(label) / 2))
    spans: Dict[str, Tuple[int, int]] = {}
    for i, (label, center) in enumerate(centers):
        left = (center + centers[i - 1][1]) / 2 if i > 0 else 0
        right = (center + centers[i + 1][1]) / 2 if i + 1 < len(centers) else len(header)
        spans[label] = (int(round(left)), int(round(right)))
    return spans


def build_html(lines: List[Text]) -> str:
    console = Console(width=TERM_COLS, color_system="truecolor", file=io.StringIO())
    plain = [line.plain for line in lines]
    header_idx = next(i for i, p in enumerate(plain) if "JobID" in p)
    spans = column_spans(plain[header_idx])

    out_rows = "\n".join(
        f'<div class="ln out-ln" id="o-{i}">{line_html(line, console)}</div>'
        for i, line in enumerate(lines)
    )
    typed = "".join(
        f'<span class="tc" id="tc-{i}">{html.escape(ch)}</span>' for i, ch in enumerate(COMMAND)
    )
    prompt = (
        '<span class="p-user">you@hpc-login</span><span class="p-sep">:</span>'
        '<span class="p-dir">~</span><span class="p-sep">$ </span>'
    )

    # Screen rows: 0 = command, 1..n = output, n+1 = blank, n+2 = new prompt.
    first_row, last_row = header_idx + 1, len(lines)
    spotlight_boxes = []
    for key, labels, _, _, _ in SPOTLIGHTS:
        left = min(spans[label][0] for label in labels)
        right = max(spans[label][1] for label in labels)
        spotlight_boxes.append(
            f'<div class="hl" id="hl-{key}" style="left:calc({left}ch + 18px - 6px);'
            f"width:calc({right - left}ch + 12px);top:{first_row * LINE_PX + 12}px;"
            f'height:{(last_row - first_row + 1) * LINE_PX + 4}px"></div>'
        )

    captions = []
    for i, (key, _, caption, start, end) in enumerate(SPOTLIGHTS):
        captions.append(
            f'<div id="cap-{key}" class="clip cap" data-start="{start}" '
            f'data-duration="{end - start:.2f}" data-track-index="{2 + i}">'
            f'<div class="cap-inner" id="cap-{key}-in">{caption}</div></div>'
        )
    tag_start = SPOTLIGHTS[-1][4] + 0.2
    captions.append(
        f'<div id="cap-tag" class="clip cap" data-start="{tag_start}" '
        f'data-duration="{DURATION - tag_start:.2f}" data-track-index="{2 + len(SPOTLIGHTS)}">'
        f'<div class="cap-inner cap-tag" id="cap-tag-in">{TAGLINE}</div></div>'
    )

    screen_rows = len(lines) + 3
    screen_h = screen_rows * LINE_PX + 24
    term_w = f"calc({TERM_COLS}ch + 36px)"

    type_times = []
    t = TYPE_START
    for i in range(len(COMMAND)):
        type_times.append(round(t, 3))
        t += 0.11 if i % 2 else 0.15
    enter_t = round(t + 0.45, 3)
    out_end = OUTPUT_START + LINE_STAGGER * len(lines) + 0.2

    blink_before = [0.85, 1.1]
    blink_after = [round(out_end + 0.5 * k, 3) for k in range(int((DURATION - out_end) / 0.5))]

    spot_js = "\n".join(
        f'tl.fromTo("#hl-{key}", {{opacity: 0}}, {{opacity: 1, duration: 0.35, ease: "power2.out"}}, {start});\n'
        f'tl.to("#hl-{key}", {{opacity: 0, duration: 0.3, ease: "power2.in"}}, {end - 0.35});\n'
        f'tl.fromTo("#cap-{key}-in", {{opacity: 0, y: 14}}, {{opacity: 1, y: 0, duration: 0.35, ease: "power3.out"}}, {start});\n'
        f'tl.to("#cap-{key}-in", {{opacity: 0, y: -8, duration: 0.25, ease: "power2.in"}}, {end - 0.3});'
        for key, _, _, start, end in SPOTLIGHTS
    )

    return f"""<!-- Generated by scripts/build_demo_video.py; edit the script, not this file. -->
<template>
    <style>
      #root {{
        position: absolute; inset: 0; background: var(--ground); font-family: var(--font-ui);
        --ground: #05070d;
        --term: {THEME.background_color.rgb};
        --ink: {THEME.foreground_color.rgb};
        --accent: #58a6ff;
        --font-mono: "JetBrains Mono";
        --font-ui: "Inter";
      }}
      #root * {{ margin: 0; padding: 0; box-sizing: border-box; }}
      #bg {{ position: absolute; inset: 0; }}
      #bg-glow {{
        position: absolute; inset: 0;
        background:
          radial-gradient(1200px 600px at 20% 0%, rgba(88,166,255,0.16), transparent 60%),
          radial-gradient(900px 500px at 90% 100%, rgba(63,185,80,0.12), transparent 60%);
      }}
      #term {{ position: absolute; inset: 0; z-index: 2; }}
      #term-win {{
        position: absolute; left: 50%; top: 90px; margin-left: calc(({term_w}) / -2);
        width: {term_w}; font-family: var(--font-mono); font-size: {FONT_PX}px;
        background: var(--term); border-radius: 14px; overflow: hidden;
        border: 1px solid rgba(240,246,252,0.12);
        box-shadow: 0 30px 80px rgba(0,0,0,0.55), 0 0 0 1px rgba(0,0,0,0.4);
      }}
      .bar {{
        height: 40px; display: flex; align-items: center; gap: 9px; padding: 0 16px;
        background: #161b22; border-bottom: 1px solid rgba(240,246,252,0.08);
      }}
      .bar i {{ width: 13px; height: 13px; border-radius: 50%; display: block; }}
      .bar .r {{ background: #ff5f57; }} .bar .y {{ background: #febc2e; }} .bar .g {{ background: #28c840; }}
      .bar span {{
        flex: 1; text-align: center; margin-right: 66px; color: #8b949e;
        font-family: var(--font-ui); font-size: 18px; font-weight: 600;
      }}
      #screen {{ position: relative; height: {screen_h}px; padding: 12px 18px; overflow: hidden; }}
      .ln {{ height: {LINE_PX}px; line-height: {LINE_PX}px; white-space: pre; color: var(--ink); }}
      .p-user {{ color: #3fb950; font-weight: 700; }}
      .p-dir {{ color: #58a6ff; font-weight: 700; }}
      .p-sep {{ color: var(--ink); }}
      .tc {{ display: none; color: #f0f6fc; font-weight: 700; }}
      .cursor {{
        display: inline-block; width: 1ch; height: {LINE_PX - 3}px; vertical-align: -3px;
        background: #f0f6fc;
      }}
      .hl {{
        position: absolute; border-radius: 8px; opacity: 0;
        border: 2px solid var(--accent);
        background: rgba(88,166,255,0.06);
        box-shadow: 0 0 0 4000px rgba(5,7,13,0.58), 0 0 24px rgba(88,166,255,0.45);
      }}
      .cap {{
        position: absolute; left: 0; right: 0; bottom: 34px; z-index: 5;
        display: flex; justify-content: center;
      }}
      .cap-inner {{
        padding: 14px 30px; border-radius: 999px; font-size: 30px; font-weight: 600;
        color: #f0f6fc; background: rgba(22,27,34,0.92);
        border: 1px solid rgba(88,166,255,0.55);
        box-shadow: 0 12px 40px rgba(0,0,0,0.5);
      }}
      .cap-inner b {{ font-family: var(--font-mono); color: #79c0ff; font-weight: 700; }}
      .cap-tag {{ border-color: rgba(63,185,80,0.6); }}
    </style>
    <div id="root" data-composition-id="{SCENE_ID}" data-width="1920" data-height="1080">
      <div id="bg" class="clip" data-start="0" data-duration="{DURATION}" data-track-index="0">
        <div id="bg-glow"></div>
      </div>
      <div id="term" class="clip" data-start="0" data-duration="{DURATION}" data-track-index="1">
        <div id="term-win">
          <div class="bar"><i class="r"></i><i class="y"></i><i class="g"></i><span data-layout-ignore>you@hpc-login: ~</span></div>
          <!-- Captured terminal cells: the layout audit cannot judge them. -->
          <div id="screen" data-layout-ignore>
            <div class="ln" id="cmd-line">{prompt}{typed}<span class="cursor" id="cursor1"></span></div>
{out_rows}
            <div class="ln" id="blank-line"> </div>
            <div class="ln" id="prompt2">{prompt}<span class="cursor" id="cursor2"></span></div>
            {"".join(spotlight_boxes)}
          </div>
        </div>
      </div>
      {"".join(captions)}
    </div>
    <script>
      const tl = gsap.timeline({{ paused: true }});
      tl.fromTo("#bg-glow", {{ opacity: 0 }}, {{ opacity: 1, duration: 1.2, ease: "power2.out" }}, 0);
      tl.fromTo("#term-win", {{ y: 10, scale: 0.992 }},
        {{ y: 0, scale: 1, duration: 0.9, ease: "power3.out" }}, 0);
      tl.set("#prompt2", {{ opacity: 0 }}, 0);
      tl.set(".out-ln", {{ opacity: 0 }}, 0);
      {"".join(f'tl.set("#cursor1", {{ opacity: {k % 2} }}, {bt});' for k, bt in enumerate(blink_before))}
      {"".join(f'tl.set("#tc-{i}", {{ display: "inline" }}, {tt});' for i, tt in enumerate(type_times))}
      tl.set("#cursor1", {{ opacity: 0 }}, {enter_t});
      tl.fromTo(".out-ln", {{ opacity: 0, y: 6 }},
        {{ opacity: 1, y: 0, duration: 0.2, ease: "power2.out", stagger: {LINE_STAGGER} }}, {OUTPUT_START});
      tl.set("#prompt2", {{ opacity: 1 }}, {out_end:.3f});
      {"".join(f'tl.set("#cursor2", {{ opacity: {(k + 1) % 2} }}, {bt});' for k, bt in enumerate(blink_after))}
{spot_js}
      tl.fromTo("#cap-tag-in", {{ opacity: 0, y: 14 }},
        {{ opacity: 1, y: 0, duration: 0.4, ease: "power3.out" }}, {tag_start});
      window.__timelines["{SCENE_ID}"] = tl;
    </script>
</template>
"""


ROOT_HTML = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=1920, height=1080" />
    <!-- Generated by scripts/build_demo_video.py; edit the script, not this file. -->
    <script src="https://cdn.jsdelivr.net/npm/gsap@3.15.0/dist/gsap.min.js"></script>
    <style>
      * {{ margin: 0; padding: 0; box-sizing: border-box; }}
      html, body {{ width: 1920px; height: 1080px; overflow: hidden; background: #05070d; }}
    </style>
  </head>
  <body>
    <div
      id="root"
      data-composition-id="main"
      data-start="0"
      data-duration="{DURATION}"
      data-width="1920"
      data-height="1080"
    >
      <div
        id="{SCENE_ID}"
        data-composition-id="{SCENE_ID}"
        data-composition-src="compositions/{SCENE_ID}.html"
        data-start="0"
        data-duration="{DURATION}"
        data-track-index="1"
        data-track-kind="graphics"
        data-width="1920"
        data-height="1080"
      ></div>
    </div>
    <script>
      window.__timelines["main"] = gsap.timeline({{ paused: true }});
    </script>
  </body>
</html>
"""


def main() -> None:
    scene = PROJECT / "compositions" / f"{SCENE_ID}.html"
    scene.parent.mkdir(parents=True, exist_ok=True)
    scene.write_text(build_html(capture_cli_output()))
    (PROJECT / "index.html").write_text(ROOT_HTML)
    print(f"wrote {scene.relative_to(ROOT)} and index.html")


if __name__ == "__main__":
    main()
