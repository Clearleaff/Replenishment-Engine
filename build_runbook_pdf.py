#!/usr/bin/env python3
"""
build_runbook_pdf.py
Compiles the Supply Chain AI Hybrid Orchestrator Manual into a styled PDF.
Requirements: pip install reportlab
"""

import sys
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Preformatted
)

def generate_pdf(output_filename="SupplyChain_AI_Hybrid_Orchestrator_Master_Runbook.pdf"):
    doc = SimpleDocTemplate(
        output_filename,
        pagesize=letter,
        rightMargin=40, leftMargin=40,
        topMargin=40, bottomMargin=40
    )

    styles = getSampleStyleSheet()
    
    # Custom Palette & Styles
    primary_color = colors.HexColor("#1e293b")
    accent_color = colors.HexColor("#0f766e")
    code_bg = colors.HexColor("#f8fafc")
    
    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Heading1'],
        fontName='Helvetica-Bold',
        fontSize=20,
        leading=24,
        textColor=primary_color,
        spaceAfter=10
    )
    
    h1_style = ParagraphStyle(
        'Heading1_Custom',
        parent=styles['Heading2'],
        fontName='Helvetica-Bold',
        fontSize=12,
        leading=16,
        textColor=accent_color,
        spaceBefore=10,
        spaceAfter=6,
        keepWithNext=True
    )

    h2_style = ParagraphStyle(
        'Heading2_Custom',
        parent=styles['Heading3'],
        fontName='Helvetica-Bold',
        fontSize=10,
        leading=13,
        textColor=primary_color,
        spaceBefore=6,
        spaceAfter=3,
        keepWithNext=True
    )
    
    body_style = ParagraphStyle(
        'Body_Custom',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=8.5,
        leading=12,
        textColor=colors.HexColor("#334155"),
        spaceAfter=5
    )

    code_style = ParagraphStyle(
        'Code_Custom',
        parent=styles['Code'],
        fontName='Courier',
        fontSize=7.5,
        leading=9.5,
        textColor=colors.HexColor("#0f172a")
    )

    story = []

    # Title & Metadata
    story.append(Paragraph("Supply Chain AI Hybrid Orchestrator", title_style))
    story.append(Paragraph("<b>End-to-End Enterprise Architecture & Operational Runbook</b>", body_style))
    story.append(Paragraph("<i>Target Environment: .NET eShop, Aspire, RabbitMQ, Rust Data Platform & Groq AI</i>", body_style))
    story.append(Spacer(1, 8))

    # Chapter 1
    story.append(Paragraph("Chapter 1: Architecture & The Hybrid Pattern", h1_style))
    story.append(Paragraph(
        "Autonomous LLMs present severe non-deterministic risks if given raw API write access. "
        "The Hybrid Architecture enforces a deterministic <b>PolicyGate</b> and feature layer around a sandboxed "
        "LLM reasoning agent (Groq / Qwen-27B). The LLM is restricted to proposing quantities; execution is guarded "
        "by business invariant rules and human authorization webhooks.",
        body_style
    ))
    story.append(Spacer(1, 6))
    
    # Chapter 2
    story.append(Paragraph("Chapter 2: Host Prerequisites Checklist", h1_style))
    story.append(Paragraph("Verify dependencies before running:", body_style))
    story.append(Preformatted("""$ docker --version          # Container runtime
$ dotnet --version          # .NET 8 / 9 / 10 SDK
$ aspire version            # .NET Aspire CLI
$ cargo --version           # Rust Toolchain
$ python3 --version         # Python Virtual Environment""", code_style))
    story.append(Spacer(1, 6))

    # Chapter 3
    story.append(Paragraph("Chapter 3: Starting Aspire & Wave Load Generator", h1_style))
    story.append(Paragraph("Execute from the repository root to start eShop and synthetic demand spikes:", body_style))
    story.append(Preformatted("""cd ~/eShop
env 'Parameters__clickhouse-password=local-dev-only' \\
DataPlatform__Enabled=true \\
DataPlatform__AgentMode=AUTO_DEMO \\
OrderGenerator__Enabled=true \\
OrderGenerator__RatePerSecond=10 \\
OrderGenerator__Continuous=true \\
OrderGenerator__VariableRate=true \\
OrderGenerator__WaveAmplitudeFraction=0.5 \\
OrderGenerator__WavePeriodSeconds=120 \\
OrderGenerator__MinimumRatePerSecond=5 \\
OrderGenerator__Maximum_RATE_PER_SECOND=15 \\
OrderGenerator__Profile=REGIONAL_SPIKE \\
OrderGenerator__TargetSkuId=42 \\
ESHOP_USE_HTTP_ENDPOINTS=1 \\
aspire start --apphost src/eShop.AppHost/eShop.AppHost.csproj""", code_style))
    story.append(Spacer(1, 6))

    # Parameters Table
    table_data = [
        ["Parameter", "Configured Value", "Operational Rationale"],
        ["RatePerSecond", "10", "Base velocity: 600 orders/minute."],
        ["Continuous", "true", "Infinite loop testing."],
        ["VariableRate", "true", "Sinusoidal wave to simulate sudden rush."],
        ["Wave Bounds", "5 to 15 /sec", "Traffic oscillates between 300 and 900 orders/min."],
        ["Profile & SKU", "REGIONAL_SPIKE / 42", "Concentrates load onto NCR warehouse festive item."]
    ]
    t = Table(table_data, colWidths=[110, 100, 320])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#0f766e")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))

    # Chapter 4 & 5
    story.append(Paragraph("Chapter 4 & 5: RabbitMQ Discovery & Rust Orchestrator Execution", h1_style))
    story.append(Paragraph("Aspire binds containers to dynamic ports. Discover credentials and run the Rust binary:", body_style))
    story.append(Preformatted("""# 1. Discover Port & Credentials
$ aspire describe eventbus --format Json

# 2. Export & Start Orchestrator
$ cd ~/eShop/src/RustDataPlatform
$ export AMQP_URL="amqp://guest:<PASSWORD>@127.0.0.1:<PORT>/%2f"
$ export GROQ_API_KEY="gsk_..."
$ export GROQ_MODEL="qwen/qwen3.8-27b"
$ export LLM_TIMEOUT_MILLISECONDS=15000
$ export RUST_LOG="hybrid_orchestrator=info,info"
$ cargo run -p hybrid-orchestrator""", code_style))
    story.append(Spacer(1, 8))

    # Chapter 6 & 7
    story.append(Paragraph("Chapter 6 & 7: Demand Injection & Boss's Desk Approval", h1_style))
    story.append(Paragraph(
        "Spike events are delivered directly into RabbitMQ. If stock drops below threshold, the LLM creates a proposal. "
        "If the deficit is high, status transitions to <b>REQUIRES_HUMAN_APPROVAL</b>.",
        body_style
    ))
    story.append(Preformatted("""# Query proposals
$ curl -sS http://127.0.0.1:5000/api/v1/proposals | jq '.[0]'

# Authorize replenishment via Boss's Desk webhook
$ curl -X POST http://127.0.0.1:5000/api/v1/proposals/<PROPOSAL_ID>/approve | jq""", code_style))
    story.append(Spacer(1, 8))

    # Chapter 8
    story.append(Paragraph("Chapter 8: Troubleshooting Matrix", h1_style))
    trouble_data = [
        ["Symptom", "Root Cause", "Action"],
        ["IOError: invalid port", "Literal <PORT> in string", "Read exact port from aspire describe eventbus."],
        ["Groq HTTP 404", "Model ID deprecated", "Use active model: export GROQ_MODEL='qwen/qwen3.8-27b'."],
        ["Proposal REJECTED", "Zero deficit", "Ensure balance payload has onHand < safetyStock."],
        ["Groq HTTP 429", "API rate limit reached", "Circuit breaker automatically trips to deterministic fallback."]
    ]
    t2 = Table(trouble_data, colWidths=[110, 150, 270])
    t2.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#1e293b")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor("#cbd5e1")),
    ]))
    story.append(t2)

    doc.build(story)
    print(f"✅ PDF successfully generated: {output_filename}")

if __name__ == "__main__":
    generate_pdf()

