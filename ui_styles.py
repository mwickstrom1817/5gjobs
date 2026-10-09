import streamlit as st


def inject_global_styles():
    """Inject responsive, mobile-first CSS once per app run.

    Keeps the existing dark theme (#0e0e10 / #18181b / #27272a / red accent)
    but enlarges touch targets on phones and stacks multi-column forms so
    they remain usable on narrow screens. Desktop is intentionally left
    visually similar to the current layout.
    """
    st.markdown(
        """
        <style>
        :root {
          --bg-page: #0e0e10;
          --bg-card: #18181b;
          --bg-input: #27272a;
          --accent: #b91c1c;
          --accent-hover: #ef4444;
          --text: #fafafa;
          --text-muted: #a1a1aa;
          --radius: 10px;
          --tap-target: 46px;
          --form-gap: 12px;
        }

        /* ------------------------------------------------------------------
           Shared typography / spacing
           ------------------------------------------------------------------ */
        .main .block-container {
          padding-top: 1.5rem;
          padding-bottom: 2rem;
        }

        /* ------------------------------------------------------------------
           Job cards
           ------------------------------------------------------------------ */
        .job-card {
          background: var(--bg-card);
          border-radius: var(--radius);
          padding: 16px;
          margin-bottom: 12px;
          border: 1px solid #27272a;
          transition: transform 0.08s ease, box-shadow 0.08s ease;
        }
        .job-card:hover {
          box-shadow: 0 4px 14px rgba(0,0,0,0.28);
        }

        /* ------------------------------------------------------------------
           Form rows: side-by-side on desktop, stacked on mobile.
           The marker div is emitted immediately before the st.columns block.
           ------------------------------------------------------------------ */
        .form-row + div[data-testid="stHorizontalBlock"] {
          flex-wrap: wrap !important;
          gap: var(--form-gap) !important;
          align-items: flex-start !important;
        }
        .form-row + div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
          min-width: 220px !important;
        }

        /* ------------------------------------------------------------------
           Top bar: logo/search/actions inline on desktop, stacked on mobile.
           ------------------------------------------------------------------ */
        .top-bar + div[data-testid="stHorizontalBlock"] {
          display: flex !important;
          gap: 12px !important;
          flex-wrap: wrap !important;
          align-items: center !important;
        }
        .top-bar + div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
          flex: 1 1 auto !important;
          min-width: 180px !important;
          width: auto !important;
        }

        /* ------------------------------------------------------------------
           Job Details dialog
           ------------------------------------------------------------------ */
        .job-status-badge {
          display: inline-block;
          color: white;
          padding: 6px 16px;
          border-radius: 999px;
          font-weight: 600;
          font-size: 0.95em;
          text-align: center;
        }
        .job-details-sections + div [role="radiogroup"] {
          display: flex !important;
          flex-wrap: wrap !important;
          gap: 8px !important;
        }
        .job-details-sections + div [role="radiogroup"] > * {
          flex: 1 1 auto !important;
          min-width: 100px !important;
          justify-content: center !important;
        }

        /* ------------------------------------------------------------------
           Tab navigation
           ------------------------------------------------------------------ */
        button[data-baseweb="tab"] {
          min-height: 40px;
          font-size: 15px;
          padding-left: 14px;
          padding-right: 14px;
        }

        /* ------------------------------------------------------------------
           Mobile overrides (max-width: 768px)
           ------------------------------------------------------------------ */
        @media (max-width: 768px) {
          /* Larger base text so forms are readable without zooming */
          .main .block-container {
            font-size: 16px;
          }

          /* Bigger touch targets for all buttons, inputs, selects, textareas */
          .stButton > button,
          [data-testid="stBaseButton-secondary"],
          [data-testid="stBaseButton-primary"],
          [data-testid="stBaseButton-ghost"] {
            min-height: var(--tap-target) !important;
            font-size: 16px !important;
            padding: 8px 16px !important;
          }
          input[type="text"],
          input[type="email"],
          input[type="tel"],
          input[type="number"],
          input[type="password"],
          input[type="search"],
          textarea,
          select,
          [data-baseweb="select"] {
            min-height: var(--tap-target) !important;
            font-size: 16px !important;
          }

          /* Stack top-bar columns fully */
          .top-bar + div[data-testid="stHorizontalBlock"] {
            flex-direction: column !important;
            align-items: stretch !important;
          }
          .top-bar + div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
            flex: 1 1 100% !important;
            min-width: 100% !important;
            width: 100% !important;
          }

          /* Stack form rows fully */
          .form-row + div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] {
            flex: 1 1 100% !important;
            min-width: 100% !important;
            width: 100% !important;
            max-width: 100% !important;
          }

          /* Job Details: full-width status badge and tile-like section nav */
          .job-status-badge {
            display: block !important;
            width: 100% !important;
            padding: 10px 16px !important;
            font-size: 1.05em !important;
          }
          .job-details-sections + div [role="radiogroup"] {
            gap: 10px !important;
          }
          .job-details-sections + div [role="radiogroup"] > * {
            flex: 1 1 45% !important;
            min-height: 56px !important;
            font-size: 15px !important;
            flex-direction: column !important;
            align-items: center !important;
            justify-content: center !important;
            text-align: center !important;
            line-height: 1.2 !important;
          }

          /* Job cards: roomier, larger title, full-width action buttons */
          .job-card {
            padding: 18px;
            margin-bottom: 16px;
          }
          .job-card .job-card-title {
            font-size: 1.15em !important;
            line-height: 1.35 !important;
            height: auto !important;
            -webkit-line-clamp: 3 !important;
          }
          .job-card .job-card-meta {
            font-size: 0.95em !important;
          }

          /* Tabs: larger, easier to tap */
          button[data-baseweb="tab"] {
            min-height: 48px;
            font-size: 16px;
            padding-left: 12px;
            padding-right: 12px;
          }

          /* Expander / section headers */
          [data-testid="stExpander"] > details > summary {
            min-height: 44px;
            font-size: 16px;
          }

          /* File uploader */
          [data-testid="stFileUploaderDropzone"] {
            min-height: 64px;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def form_row(weights=None, gap="12px"):
    """Emit a marker div so the next st.columns block is styled as a form row.

    Usage:
        c1, c2 = form_row([1, 1])
        c1.text_input(...)
        c2.selectbox(...)

    On desktop the columns sit side-by-side; on narrow screens they stack
    vertically with full-width inputs.
    """
    st.markdown(
        f'<div class="form-row" style="--form-gap:{gap}"></div>',
        unsafe_allow_html=True,
    )
    return st.columns(weights) if weights else st.columns(2)
