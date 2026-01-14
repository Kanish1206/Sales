import streamlit as st
import pandas as pd
from sales_processor import SalesProcessor
import io

# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------
st.set_page_config(
    page_title="Sales Engine",
    page_icon="🚀",
    layout="wide"
)

# --------------------------------------------------
# CUSTOM CSS (CLEAN & PROFESSIONAL)
# --------------------------------------------------
st.markdown("""
<style>
.main-title {
    font-size: 36px;
    font-weight: 700;
    margin-bottom: 5px;
}
.sub-title {
    color: #6b7280;
    margin-bottom: 25px;
}
.card {
    background-color: #0e1117;
    padding: 20px;
    border-radius: 12px;
    border: 1px solid #1f2937;
}
.upload-label {
    font-weight: 600;
    margin-bottom: 5px;
}
</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------
st.markdown('<div class="main-title">🚀 Sales & Master Processing Engine</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">Upload your files and generate a clean, analysis-ready report in one click.</div>', unsafe_allow_html=True)

# --------------------------------------------------
# UPLOAD SECTION (MAIN PAGE)
# --------------------------------------------------
st.markdown("### 📤 Upload Files")

c1, c2, c3 = st.columns([3, 3, 2])

with c1:
    st.markdown('<div class="upload-label">Q3 Sales File (.xlsb / .xlsx)</div>', unsafe_allow_html=True)
    s_file = st.file_uploader(
        "Sales File",
        type=["xlsb", "xlsx"],
        label_visibility="collapsed"
    )

with c2:
    st.markdown('<div class="upload-label">Master File (.xlsx)</div>', unsafe_allow_html=True)
    m_file = st.file_uploader(
        "Master File",
        type=["xlsx"],
        label_visibility="collapsed"
    )

with c3:
    st.markdown('<div class="upload-label">Run</div>', unsafe_allow_html=True)
    run_process = st.button("⚡ Generate Report", use_container_width=True)

st.divider()

# --------------------------------------------------
# PROCESSING LOGIC
# --------------------------------------------------
if run_process:

    if not s_file or not m_file:
        st.error("❌ Upload both Sales and Master files before running the engine.")
        st.stop()

    with st.spinner("Executing transformation pipeline..."):
        engine = SalesProcessor(s_file, m_file)
        raw_data, pivot_summary = engine.process()

    st.success("✅ Processing completed successfully")

    # --------------------------------------------------
    # RESULTS SECTION
    # --------------------------------------------------
    tab1, tab2 = st.tabs(["📊 Pivot Summary", "📋 Detailed Sales Data"])

    with tab1:
        st.subheader("Pivot Summary")
        st.dataframe(pivot_summary.to_pandas(), use_container_width=True)

    with tab2:
        st.subheader("Sales Data (Preview)")
        st.dataframe(raw_data.to_pandas().head(200), use_container_width=True)

    # --------------------------------------------------
    # EXPORT
    # --------------------------------------------------
    st.divider()
    st.markdown("### 📥 Export")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        raw_data.to_pandas().to_excel(writer, sheet_name="Sales", index=False)
        pivot_summary.to_pandas().to_excel(writer, sheet_name="Pivot", index=False)

    st.download_button(
        "⬇️ Download Final Excel Report",
        data=output.getvalue(),
        file_name="Sales_Report.xlsx",
        use_container_width=True
    )

else:
    st.info("👆 Upload both files and click **Generate Report** to begin.")
