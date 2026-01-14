import streamlit as st
import pandas as pd
import io
from sales_processor import SalesProcessor

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Sales Engine",
    page_icon="📊",
    layout="wide"
)

# --------------------------------------------------
# BASIC CLEAN CSS
# --------------------------------------------------
st.markdown("""
<style>
.block-container { padding-top: 2rem; }
h1, h2, h3 { font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------
st.title("🚀 Sales & Master Processing Engine")
st.markdown(
    "Upload sales and master files to generate **detailed transactional data** "
    "followed by an **aggregated pivot summary**."
)

st.divider()

# --------------------------------------------------
# FILE UPLOAD SECTION (MAIN PAGE)
# --------------------------------------------------
st.subheader("📂 Upload Files")

c1, c2 = st.columns(2)

with c1:
    st.markdown("#### 🧾 Sales File")
    s_file = st.file_uploader(
        "Upload Sales (.xlsb / .xlsx)",
        type=["xlsb", "xlsx"],
        label_visibility="collapsed"
    )

with c2:
    st.markdown("#### 📘 Master File")
    m_file = st.file_uploader(
        "Upload Master (.xlsx)",
        type=["xlsx"],
        label_visibility="collapsed"
    )

st.markdown("<br>", unsafe_allow_html=True)

run_process = st.button(
    "⚙️ Generate Report",
    type="primary",
    use_container_width=True
)

# --------------------------------------------------
# PROCESSING
# --------------------------------------------------
if run_process:
    if not s_file or not m_file:
        st.error("❌ Upload BOTH Sales and Master files.")
    else:
        with st.spinner("Running data pipeline..."):
            engine = SalesProcessor(s_file, m_file)
            raw_data, pivot_summary = engine.process()

        st.success("✅ Processing completed.")

        # --------------------------------------------------
        # METRICS (TRUST INDICATOR)
        # --------------------------------------------------
        m1, m2 = st.columns(2)
        m1.metric("Detailed Rows", raw_data.height)
        m2.metric("Pivot Rows", pivot_summary.height)

        st.divider()

        # --------------------------------------------------
        # RESULT TABS (LOGICALLY ORDERED)
        # --------------------------------------------------
        t1, t2 = st.tabs([
            "📋 Detailed Sales Data",
            "📊 Pivot Summary"
        ])

        with t1:
            st.subheader("Detailed Sales Data")
            st.caption("Row-level data after master mapping")
            st.dataframe(
                raw_data.to_pandas(),
                use_container_width=True
            )

        with t2:
            st.subheader("Pivot Summary")
            st.caption("Aggregated metrics derived from detailed data")
            st.dataframe(
                pivot_summary.to_pandas(),
                use_container_width=True
            )

        # --------------------------------------------------
        # EXPORT
        # --------------------------------------------------
        st.divider()
        st.subheader("⬇️ Export Final Report")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            raw_data.to_pandas().to_excel(
                writer, sheet_name="Detailed_Sales", index=False
            )
            pivot_summary.to_pandas().to_excel(
                writer, sheet_name="Pivot_Summary", index=False
            )

        st.download_button(
            "📥 Download Excel",
            data=output.getvalue(),
            file_name="Sales_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
