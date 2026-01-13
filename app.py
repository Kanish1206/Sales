import streamlit as st
import pandas as pd
from sales_processor import SalesProcessor # Importing our Engine
import io

st.set_page_config(page_title="Sales Engine", layout="wide")

st.title("🚀 Sales Logic Processor")
st.markdown("Upload your files to apply the automated logic pipeline.")

# --- Sidebar Uploads ---
with st.sidebar:
    st.header("Data Sources")
    s_file = st.file_uploader("Upload Q3 Sales (.xlsb)", type=['xlsb', 'xlsx'])
    m_file = st.file_uploader("Upload Master (.xlsx)", type=['xlsx'])
    run_process = st.button("Generate Report")

# --- Processing Logic ---
if run_process and s_file and m_file:
    with st.spinner("Executing Logic Pipeline..."):
        # Integration Point: Calling the SalesProcessor class
        engine = SalesProcessor(s_file, m_file)
        raw_data, pivot_summary = engine.process()
        
        # Tabs for UI
        t1, t2 = st.tabs(["📊 Pivot Table", "📋 Detailed Data"])
        
        with t1:
            st.subheader("Final Pivot Summary")
            st.dataframe(pivot_summary.to_pandas(), use_container_width=True)
            
            # Export to Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                raw_data.to_pandas().to_excel(writer, sheet_name="Sales", index=False)
                pivot_summary.to_pandas().to_excel(writer, sheet_name="Pivot", index=False)
            
            st.download_button("📥 Download Final Excel", output.getvalue(), "Sales_Report.xlsx")

        with t2:
            st.dataframe(raw_data.to_pandas().head(100))

else:
    st.warning("Please upload both Sales and Master files to proceed.")