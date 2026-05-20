"""
app.py — Streamlit web UI for hialt-recall.

Run with:
    streamlit run app.py

Or with a custom port:
    streamlit run app.py --server.port 8502
"""

import streamlit as st

st.set_page_config(
    page_title="hialt-recall",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom styling ──────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:ital,wght@0,400;0,600;0,700;1,400&family=Syne:wght@400;600;700;800&display=swap');

:root {
    --bg:       #0d1117;
    --surface:  #161b22;
    --surface2: #1c2333;
    --border:   #30363d;
    --accent:   #388bfd;
    --green:    #3fb950;
    --orange:   #f0883e;
    --red:      #f85149;
    --text:     #e6edf3;
    --muted:    #8b949e;
    --mono:     'JetBrains Mono', monospace;
    --display:  'Syne', sans-serif;
}

/* ── Base ── */
html, body, [data-testid="stAppViewContainer"] {
    background-color: var(--bg) !important;
    color: var(--text) !important;
}
[data-testid="stSidebar"] {
    background-color: var(--surface) !important;
    border-right: 1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color: var(--text) !important; }

/* ── Header ── */
.recall-header {
    display: flex;
    align-items: baseline;
    gap: 14px;
    margin-bottom: 6px;
}
.recall-logo {
    font-family: var(--display);
    font-size: 2rem;
    font-weight: 800;
    color: var(--text);
    letter-spacing: -1.5px;
    line-height: 1;
}
.recall-logo span { color: var(--accent); }
.recall-tag {
    font-family: var(--mono);
    font-size: 0.7rem;
    color: var(--muted);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 2px 7px;
}
.recall-sub {
    font-family: var(--mono);
    font-size: 0.78rem;
    color: var(--muted);
    margin-bottom: 28px;
}

/* ── Input area ── */
textarea {
    background-color: var(--surface) !important;
    color: var(--text) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    font-family: var(--display) !important;
    font-size: 1rem !important;
    resize: vertical !important;
}
textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 3px rgba(56, 139, 253, 0.12) !important;
    outline: none !important;
}

/* ── Buttons ── */
.stButton > button {
    background: var(--accent) !important;
    color: #0d1117 !important;
    font-family: var(--mono) !important;
    font-weight: 700 !important;
    font-size: 0.85rem !important;
    letter-spacing: 0.5px !important;
    border: none !important;
    border-radius: 6px !important;
    padding: 10px 28px !important;
    transition: opacity 0.15s, transform 0.1s !important;
}
.stButton > button:hover { opacity: 0.88 !important; transform: translateY(-1px) !important; }
.stButton > button:active { transform: translateY(0) !important; }

/* ── Sliders ── */
.stSlider [data-baseweb="slider"] { padding-top: 6px !important; }

/* ── LLM badge ── */
.llm-badge {
    display: inline-block;
    font-family: var(--mono);
    font-size: 0.72rem;
    font-weight: 600;
    padding: 2px 9px;
    border-radius: 20px;
    margin-left: 8px;
    vertical-align: middle;
}
.llm-groq   { background: rgba(56,139,253,0.15); color: var(--accent); border: 1px solid rgba(56,139,253,0.3); }
.llm-ollama { background: rgba(63,185,80,0.12);  color: var(--green);  border: 1px solid rgba(63,185,80,0.3); }
.llm-none   { background: rgba(240,136,62,0.12); color: var(--orange); border: 1px solid rgba(240,136,62,0.3); }

/* ── Answer card ── */
.answer-wrap {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid var(--green);
    border-radius: 8px;
    padding: 22px 26px;
    margin-top: 6px;
    font-family: var(--display);
    font-size: 0.96rem;
    line-height: 1.75;
    color: var(--text);
}

/* ── Citation cards ── */
.cit-card {
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 12px 16px;
    margin-bottom: 8px;
    font-family: var(--mono);
    font-size: 0.78rem;
}
.cit-rank {
    color: var(--muted);
    font-weight: 600;
    margin-right: 6px;
}
.cit-file { color: var(--accent); font-weight: 600; }
.cit-section { color: var(--muted); }
.cit-score-bar {
    display: inline-block;
    height: 4px;
    border-radius: 2px;
    background: var(--accent);
    opacity: 0.6;
    vertical-align: middle;
    margin: 0 6px;
}
.cit-score-val { color: var(--muted); font-size: 0.72rem; }
.cit-snippet {
    margin-top: 6px;
    color: var(--muted);
    font-size: 0.75rem;
    line-height: 1.5;
    white-space: pre-wrap;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;
}

/* ── Divider ── */
hr { border-color: var(--border) !important; }

/* ── Status / alerts ── */
.stAlert { border-radius: 6px !important; font-family: var(--mono) !important; font-size: 0.82rem !important; }

/* ── Sidebar labels ── */
.sidebar-section {
    font-family: var(--mono);
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 1.5px;
    color: var(--muted);
    text-transform: uppercase;
    margin: 20px 0 8px;
}

/* ── Streamlit misc overrides ── */
[data-testid="stMarkdownContainer"] p { margin-bottom: 0.5rem; }
.block-container { padding-top: 2rem !important; max-width: 900px !important; }
footer { display: none !important; }
#MainMenu { display: none !important; }
</style>
""", unsafe_allow_html=True)

# ── Imports after page config ───────────────────────────────────────────────
from rag_engine import load_settings, run_query, Settings, RAGResult


# ── Session state ────────────────────────────────────────────────────────────
if "settings" not in st.session_state:
    try:
        st.session_state.settings = load_settings()
        st.session_state.settings_error = None
    except RuntimeError as e:
        st.session_state.settings = None
        st.session_state.settings_error = str(e)

if "history" not in st.session_state:
    st.session_state.history: list[RAGResult] = []


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="sidebar-section">Query settings</div>', unsafe_allow_html=True)

    top_k = st.slider(
        "Top-K chunks",
        min_value=1, max_value=20, value=3, step=1,
        help="Number of context chunks retrieved from MongoDB before sending to the LLM.",
    )

    force_ollama = st.toggle(
        "Force local Ollama (skip Groq)",
        value=False,
        help="Mirrors the --no-groq CLI flag. Useful when you're offline or want to avoid Groq.",
    )

    st.markdown('<div class="sidebar-section">Environment</div>', unsafe_allow_html=True)
    cfg = st.session_state.settings
    if cfg:
        st.markdown(f"""
<div style="font-family: var(--mono); font-size: 0.72rem; color: var(--muted); line-height: 1.9;">
  <b style="color:var(--text);">DB</b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{cfg.mongo_default_db}<br>
  <b style="color:var(--text);">Col</b>&nbsp;&nbsp;&nbsp;&nbsp;{cfg.mongo_collection}<br>
  <b style="color:var(--text);">Embed</b>&nbsp;&nbsp;{cfg.embedding_model}<br>
  <b style="color:var(--text);">Groq</b>&nbsp;&nbsp;&nbsp;{'✓ ' + cfg.groq_model if cfg.groq_api_key else '✗ not configured'}<br>
  <b style="color:var(--text);">Ollama</b>&nbsp;{cfg.ollama_llm_model}
</div>
""", unsafe_allow_html=True)
    else:
        st.error("Config not loaded")

    if st.session_state.history:
        st.markdown('<div class="sidebar-section">History</div>', unsafe_allow_html=True)
        for i, r in enumerate(reversed(st.session_state.history[-10:]), 1):
            badge = "🟢" if r.llm_used == "groq" else ("🟡" if r.llm_used == "ollama" else "🔴")
            truncated = r.question[:42] + "…" if len(r.question) > 42 else r.question
            st.markdown(
                f'<div style="font-family:var(--mono);font-size:0.72rem;color:var(--muted);'
                f'margin-bottom:4px;">{badge} {truncated}</div>',
                unsafe_allow_html=True,
            )


# ── Main content ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="recall-header">
  <span class="recall-logo">hialt<span>-recall</span></span>
  <span class="recall-tag">local-first RAG</span>
</div>
<div class="recall-sub">Ask questions about your codebase. Grounded answers, cited sources.</div>
""", unsafe_allow_html=True)

# Config error banner
if st.session_state.settings_error:
    st.error(f"⚠️ Configuration error: {st.session_state.settings_error}\n\nCheck that `.env` exists and `MONGO_URI` is set.")
    st.stop()

# Query input
question = st.text_area(
    label="Question",
    placeholder='e.g. "What MQTT topic does foxwatch subscribe to?" or "How does ingest.py chunk Markdown files?"',
    height=90,
    label_visibility="collapsed",
)

col_btn, col_hint = st.columns([1, 4])
with col_btn:
    submit = st.button("→ Ask", use_container_width=True)
with col_hint:
    st.markdown(
        f'<div style="font-family:var(--mono);font-size:0.72rem;color:var(--muted);padding-top:10px;">'
        f'top-k={top_k} &nbsp;·&nbsp; '
        f'{"ollama only" if force_ollama else "groq → ollama fallback"}'
        f'</div>',
        unsafe_allow_html=True,
    )

# ── Run query ─────────────────────────────────────────────────────────────────
if submit:
    question = question.strip()
    if not question:
        st.warning("Please enter a question.")
        st.stop()

    with st.spinner("🔍 Embedding · retrieving · generating…"):
        result = run_query(
            question=question,
            top_k=top_k,
            force_ollama=force_ollama,
            settings=st.session_state.settings,
        )

    st.session_state.history.append(result)

    # ── Fallback / error notices ─────────────────────────────────────────────
    if result.error and result.answer:
        st.warning(result.error)
    elif result.error and not result.answer:
        st.error(f"Query failed: {result.error}")
        st.stop()

    # ── LLM badge ────────────────────────────────────────────────────────────
    badge_cls = f"llm-{result.llm_used}"
    badge_label = {"groq": "⚡ Groq", "ollama": "🦙 Ollama", "none": "⚠ none"}.get(result.llm_used, result.llm_used)
    st.markdown(
        f'<div style="font-family:var(--mono);font-size:0.8rem;color:var(--muted);margin-bottom:4px;">'
        f'Answer <span class="llm-badge {badge_cls}">{badge_label}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Answer ───────────────────────────────────────────────────────────────
    with st.container():
        st.markdown(f'<div class="answer-wrap">', unsafe_allow_html=True)
        st.markdown(result.answer)
        st.markdown("</div>", unsafe_allow_html=True)

    # ── Citations ─────────────────────────────────────────────────────────────
    if result.chunks:
        st.markdown("---")
        st.markdown(
            f'<div style="font-family:var(--mono);font-size:0.72rem;font-weight:600;'
            f'letter-spacing:1.2px;text-transform:uppercase;color:var(--muted);margin-bottom:10px;">'
            f'Sources · {len(result.chunks)} chunk{"s" if len(result.chunks) != 1 else ""} retrieved'
            f'</div>',
            unsafe_allow_html=True,
        )

        for i, chunk in enumerate(result.chunks, 1):
            # Score bar width: 0–100px proportional to score (typical range 0.3–0.95)
            bar_width = max(8, int(chunk["score"] * 110))
            snippet = chunk["text"][:220].replace("\n", " ").strip()
            if len(chunk["text"]) > 220:
                snippet += " …"

            section_html = (
                f'<span class="cit-section"> § {chunk["headers"]}</span>'
                if chunk["headers"] and chunk["headers"] != "Code"
                else (
                    '<span class="cit-section"> · code</span>'
                    if chunk["headers"] == "Code"
                    else ""
                )
            )

            st.markdown(f"""
<div class="cit-card">
  <span class="cit-rank">[{i}]</span>
  <span class="cit-file">{chunk['source_file']}</span>
  {section_html}
  <span class="cit-score-bar" style="width:{bar_width}px;"></span>
  <span class="cit-score-val">{chunk['score']:.3f}</span>
  <div class="cit-snippet">{snippet}</div>
</div>
""", unsafe_allow_html=True)
