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
# CUSTOM CSS (Clean, Professional)
# --------------------------------------------------
st.markdown("""
<style>
.block-container {
    padding-top: 2rem;
}

.upload-box {
    border: 2px dashed #4B8BBE;
    border-radius: 12px;
    padding: 20px;
    background-color: #F7FAFC;
}

h1, h2, h3 {
    font-weight: 600;
}
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------
st.title("🚀 Sales & Master Processing Engine")
st.markdown(
    "Automated pipeline to **clean, enrich, and summarize sales data** using the master file."
)

st.divider()

# --------------------------------------------------
# MAIN UPLOAD SECTION
# --------------------------------------------------
st.subheader("📂 Upload Required Files")

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🧾 Sales Data")
    s_file = st.file_uploader(
        "Upload Q3 Sales File",
        type=["xlsb", "xlsx"],
        label_visibility="collapsed"
    )

with col2:
    st.markdown("#### 📘 Master Data")
    m_file = st.file_uploader(
        "Upload Master File",
        type=["xlsx"],
        label_visibility="collapsed"
    )

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------
# ACTION BUTTON
# --------------------------------------------------
run_process = st.button(
    "⚙️ Generate Report",
    type="primary",
    use_container_width=True
)

# --------------------------------------------------
# PROCESSING LOGIC
# --------------------------------------------------
if run_process:
    if not s_file or not m_file:
        st.error("❌ Both Sales and Master files are mandatory.")
    else:
        with st.spinner("Running transformation pipeline..."):
            engine = SalesProcessor(s_file, m_file)
            raw_data, pivot_summary = engine.process()

        st.success("✅ Processing completed successfully.")

        # --------------------------------------------------
        # RESULT TABS
        # --------------------------------------------------
        t1, t2 = st.tabs(["📊 Pivot Summary", "📋 Detailed Sales Data"])

        with t1:
            st.subheader("Pivot Summary")
            st.dataframe(
                pivot_summary.to_pandas(),
                use_container_width=True
            )

        with t2:
            st.subheader("Raw Sales Data (Preview)")
            st.dataframe(
                raw_data.to_pandas().head(200),
                use_container_width=True
            )

        # --------------------------------------------------
        # EXPORT SECTION
        # --------------------------------------------------
        st.divider()
        st.subheader("⬇️ Export Result")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            raw_data.to_pandas().to_excel(
                writer, sheet_name="Sales", index=False
            )
            pivot_summary.to_pandas().to_excel(
                writer, sheet_name="Pivot", index=False
            )

        st.download_button(
            "📥 Download Final Excel Report",
            data=output.getvalue(),
            file_name="Sales_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )



