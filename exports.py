"""
Formatted data export: CSV, JSON, Excel (.xlsx), PDF report and chart images (PNG/JPG).
"""

from __future__ import annotations

import io
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

PALETTE = ["#2563EB", "#F97316", "#10B981", "#A855F7", "#EF4444", "#0EA5E9", "#EAB308", "#64748B"]


def _naive(df: pd.DataFrame) -> pd.DataFrame:
    """Excel and most tools cannot store timezone-aware datetimes."""
    df = df.copy()
    for c in df.columns:
        if isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].dt.tz_localize(None)
    return df


def to_csv(df: pd.DataFrame) -> bytes:
    return _naive(df).to_csv(index=False).encode("utf-8-sig")  # BOM so Excel shows °C correctly


def to_json(df: pd.DataFrame) -> bytes:
    return _naive(df).to_json(orient="records", date_format="iso", indent=2, force_ascii=False).encode("utf-8")


def to_excel(sheets: Dict[str, pd.DataFrame], info: Optional[Dict[str, str]] = None) -> bytes:
    """Multi-sheet workbook with styled headers, frozen header row, filters and fitted columns."""
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if info:
            pd.DataFrame(list(info.items()), columns=["Field", "Value"]).to_excel(
                writer, sheet_name="Report Info", index=False)
        for name, df in sheets.items():
            _naive(df).to_excel(writer, sheet_name=name[:31], index=False)

        header_fill = PatternFill("solid", fgColor="1E3A8A")
        for ws in writer.book.worksheets:
            for cell in ws[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal="center", vertical="center")
            ws.freeze_panes = "A2"
            if ws.max_row > 1:
                ws.auto_filter.ref = ws.dimensions
            for col_idx, col in enumerate(ws.iter_cols(min_row=1, max_row=min(ws.max_row, 500)), start=1):
                width = max(len(str(c.value)) if c.value is not None else 0 for c in col)
                ws.column_dimensions[get_column_letter(col_idx)].width = min(max(10, width + 2), 45)
                if col[0].value and any(k in str(col[0].value).lower() for k in ("time", "bucket", "window", "first", "last")):
                    for c in col[1:]:
                        c.number_format = "yyyy-mm-dd hh:mm"
    return buf.getvalue()


def chart_image(df: pd.DataFrame, x: str, y: str, group: str, title: str, ylabel: str,
                fmt: str = "png", dpi: int = 150) -> bytes:
    """Line chart (one line per group) rendered with matplotlib -> PNG/JPG bytes."""
    fig, ax = plt.subplots(figsize=(10, 4.8))
    data = _naive(df)
    for i, (name, g) in enumerate(data.groupby(group)):
        g = g.sort_values(x)
        ax.plot(g[x], g[y], label=str(name), color=PALETTE[i % len(PALETTE)], linewidth=1.8)
    ax.set_title(title, fontsize=13, fontweight="bold", loc="left")
    ax.set_ylabel(ylabel)
    ax.grid(alpha=0.25)
    ax.spines[["top", "right"]].set_visible(False)
    if pd.api.types.is_datetime64_any_dtype(data[x]):
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(mdates.AutoDateLocator()))
    if data[group].nunique() > 1:
        ax.legend(frameon=False, ncol=min(5, data[group].nunique()), loc="upper center",
                  bbox_to_anchor=(0.5, -0.12))
    fig.tight_layout()
    buf = io.BytesIO()
    save_fmt = "jpeg" if fmt.lower() in ("jpg", "jpeg") else "png"
    fig.savefig(buf, format=save_fmt, dpi=dpi, facecolor="white")
    plt.close(fig)
    return buf.getvalue()


def bar_image(df: pd.DataFrame, x: str, y: str, title: str, ylabel: str, fmt: str = "png") -> bytes:
    fig, ax = plt.subplots(figsize=(10, 4.2))
    d = df.sort_values(y, ascending=False)
    ax.bar(d[x].astype(str), d[y], color=PALETTE[0])
    for i, v in enumerate(d[y]):
        ax.text(i, v, f"{v:.1f}", ha="center", va="bottom", fontsize=9)
    ax.set_title(title, fontsize=13, fontweight="bold", loc="left")
    ax.set_ylabel(ylabel)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="jpeg" if fmt.lower() in ("jpg", "jpeg") else "png", dpi=150, facecolor="white")
    plt.close(fig)
    return buf.getvalue()


def pdf_report(title: str, subtitle: str, info: Dict[str, str], summary: pd.DataFrame,
               charts: List[Tuple[str, bytes]], data: Optional[pd.DataFrame] = None,
               max_rows: int = 60) -> bytes:
    """A formatted PDF report: header, filter info, summary table, charts and a data sample."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import (Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table,
                                    TableStyle)

    buf = io.BytesIO()
    page = landscape(A4)
    doc = SimpleDocTemplate(buf, pagesize=page, leftMargin=1.5 * cm, rightMargin=1.5 * cm,
                            topMargin=1.3 * cm, bottomMargin=1.3 * cm, title=title)
    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=styles["Title"], alignment=0, textColor=colors.HexColor("#1E3A8A"))
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], textColor=colors.HexColor("#1E3A8A"))
    small = ParagraphStyle("small", parent=styles["Normal"], fontSize=9, textColor=colors.HexColor("#475569"))

    def table(df: pd.DataFrame, font=8):
        df = _naive(df).copy()
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M")
            elif pd.api.types.is_float_dtype(df[c]):
                df[c] = df[c].map(lambda v: "" if pd.isna(v) else f"{v:,.2f}")
        rows = [list(map(str, df.columns))] + df.astype(str).values.tolist()
        t = Table(rows, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1E3A8A")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), font),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F1F5F9")]),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#CBD5E1")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        return t

    story = [Paragraph(title, h1), Paragraph(subtitle, small), Spacer(1, 8)]
    info = {"Generated": datetime.now().strftime("%Y-%m-%d %H:%M"), **info}
    story += [table(pd.DataFrame(list(info.items()), columns=["Field", "Value"]), font=9), Spacer(1, 10)]

    if summary is not None and not summary.empty:
        story += [Paragraph("Summary statistics", h2), table(summary), Spacer(1, 10)]

    width = page[0] - 3 * cm
    for chart_title, png in charts:
        story += [PageBreak(), Paragraph(chart_title, h2),
                  Image(io.BytesIO(png), width=width, height=width * 0.46)]

    if data is not None and not data.empty:
        story += [PageBreak(), Paragraph(f"Data sample (latest {min(max_rows, len(data))} of {len(data):,} records)", h2),
                  Paragraph("Use the Excel or CSV export for the complete dataset.", small), Spacer(1, 6),
                  table(data.tail(max_rows), font=7)]

    def footer(canvas, d):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#64748B"))
        canvas.drawString(1.5 * cm, 0.8 * cm, "Weather Streaming Pipeline - Open-Meteo / Kafka / Spark / MongoDB")
        canvas.drawRightString(page[0] - 1.5 * cm, 0.8 * cm, f"Page {d.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return buf.getvalue()
