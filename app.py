# app.py - Updated per request:
# - Business Permit Validity field auto-fills "31-Dec-<same year>" and never shows "[unclear]"
# - Removed "Refresh All" and "Reset Cache" buttons
# - Exported Excel: UI-aligned headers (spaces -> underscores) and exact order requested
#   Column set/order:
#     Document_Type
#     Page_Count
#     Name_of_file
#     Business_Name_Establishment
#     Business_Owner
#     Business_Address
#     Mayor_Name
#     Other_Official_Names
#     Other_Official_Titles
#     Municipality_City_Template
#     Permit_Number
#     Issue_Date
#     Validity_Date
#     Nature_of_Business
#     raw_text
#     cleaned_text
#
# - Rules for Other_Official_Titles:
#     If Other_Official_Names is "None" (or "", "null"), export literal "None".
#     Else, collect titles from parsed list or legacy string; if none found, export "None".

import streamlit as st
import os
import io
import json
import pandas as pd
import traceback
import time
import concurrent.futures
from datetime import datetime   # NEW: for validity computation

st.set_page_config(page_title="Business Permit Data Intelligence Engine", layout="wide", initial_sidebar_state="expanded")
st.title("🏢 Business Permit Data Intelligence Engine")

# ---------- Sidebar aesthetic tweaks + Bold field labels ----------
st.markdown("""
<style>
/* ===== Missing CSS Classes ===== */
.sb-label {
  color: #374151;
  font-weight: 600;
  font-size: 0.9rem;
  margin: 0.1rem 0;
}

.sb-help {
  color: #9CA3AF;
  font-size: 0.75rem;
  margin: 0.2rem 0 0.4rem;
  text-align: center;
  padding: 0 0.5rem;
  line-height: 1.3;
}

.sb-group {
  margin: 0.25rem 0;
}

/* ===== BOLD FIELD LABELS ===== */
div[data-testid="stTextInput"] label,
div[data-testid="stTextArea"] label {
    font-weight: 800 !important;
}

/* ===== SIDEBAR BUTTONS ===== */
[data-testid="stSidebar"] .stButton,
[data-testid="stSidebar"] .stDownloadButton {
  width: 100% !important;
  margin: .25rem 0 !important;
}

[data-testid="stSidebar"] .stButton > button,
[data-testid="stSidebar"] .stDownloadButton > button,
[data-testid="stSidebar"] .stButton button[kind="primary"],
[data-testid="stSidebar"] .stButton button[kind="secondary"],
[data-testid="stSidebar"] .stDownloadButton button[kind="primary"],
[data-testid="stSidebar"] .stDownloadButton button[kind="secondary"],
[data-testid="stSidebar"] .stButton button,
[data-testid="stSidebar"] .stDownloadButton button {
  display: block !important;
  width: 100% !important;
  min-width: 100% !important;
  max-width: 100% !important;
  box-sizing: border-box !important;
  height: 2.8rem !important;
  padding: 0 1rem !important;
  border-radius: 12px !important;
  margin: 0 !important;
  background: #FFFFFF !important;
  border: 1px solid #E5E7EB !important;
  color: #1F2937 !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important;
  transition: all 0.2s ease !important;
  font-weight: 500 !important;
  font-size: 0.95rem !important;
  text-align: center !important;
  white-space: nowrap !important;
  overflow: hidden !important;
  text-overflow: ellipsis !important;
}

[data-testid="stSidebar"] .stButton > button:hover,
[data-testid="stSidebar"] .stDownloadButton > button:hover,
[data-testid="stSidebar"] .stButton button:hover,
[data-testid="stSidebar"] .stDownloadButton button:hover {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  color: #FFFFFF !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 8px rgba(191, 67, 66, 0.3) !important;
}

[data-testid="stSidebar"] .stButton > button:active,
[data-testid="stSidebar"] .stDownloadButton > button:active,
[data-testid="stSidebar"] .stButton button:active,
[data-testid="stSidebar"] .stDownloadButton button:active {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  color: #FFFFFF !important;
  transform: translateY(0px) !important;
  box-shadow: 0 2px 4px rgba(191, 67, 66, 0.4) !important;
}

/* Primary Download Button in Sidebar */
[data-testid="stSidebar"] .stDownloadButton:first-of-type > button {
  background: #f54f4f !important;
  border-color: #f54f4f !important;
  color: #FFFFFF !important;
}

[data-testid="stSidebar"] .stDownloadButton:first-of-type > button:hover {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  color: #FFFFFF !important;
}

/* ===== MAIN CONTENT BUTTONS ===== */
.stButton > button[kind="primary"],
.stDownloadButton > button,
button[data-testid*="update_record"],
button[data-testid*="download_excel"],
button[data-testid*="dl_cleaned"],
button[data-testid*="dl_raw"] {
  display: inline-block !important;
  height: 2.5rem !important;
  padding: 0 1.5rem !important;
  border-radius: 8px !important;
  background: #f54f4f !important;
  border: 1px solid #f54f4f !important;
  color: #FFFFFF !important;
  box-shadow: 0 2px 4px rgba(245, 79, 79, 0.2) !important;
  transition: all 0.2s ease !important;
  font-weight: 500 !important;
  font-size: 0.9rem !important;
  text-align: center !important;
  cursor: pointer !important;
}

.stButton > button[kind="primary"]:hover,
.stDownloadButton > button:hover,
button[data-testid*="update_record"]:hover,
button[data-testid*="download_excel"]:hover,
button[data-testid*="dl_cleaned"]:hover,
button[data-testid*="dl_raw"]:hover {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 8px rgba(191, 67, 66, 0.3) !important;
}

/* Secondary buttons */
.stButton > button[kind="secondary"],
button[data-testid*="reprocess"],
button[data-testid*="clear"] {
  display: inline-block !important;
  height: 2.5rem !important;
  padding: 0 1.5rem !important;
  border-radius: 8px !important;
  background: #FFFFFF !important;
  border: 1px solid #E5E7EB !important;
  color: #1F2937 !important;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important;
  transition: all 0.2s ease !important;
  font-weight: 500 !important;
  font-size: 0.9rem !important;
  text-align: center !important;
  cursor: pointer !important;
}

.stButton > button[kind="secondary"]:hover,
button[data-testid*="reprocess"]:hover,
button[data-testid*="clear"]:hover {
  background: #BF4342 !important;
  border-color: #BF4342 !important;
  color: #FFFFFF !important;
  transform: translateY(-1px) !important;
  box-shadow: 0 4px 8px rgba(191, 67, 66, 0.3) !important;
}

/* Container fixes */
[data-testid="stSidebar"] .element-container,
[data-testid="stSidebar"] .stButton .element-container,
[data-testid="stSidebar"] .stDownloadButton .element-container {
  width: 100% !important;
}

[data-testid="stSidebar"] .stButton,
[data-testid="stSidebar"] .stDownloadButton {
  flex: none !important;
}

[data-testid="stSidebar"] button[data-testid] {
  width: 100% !important;
  min-width: 100% !important;
}
</style>
""", unsafe_allow_html=True)

# ---------- Import processing helpers ----------
MAIN_AVAILABLE = True
_import_error = None
try:
    from main import process_pdf, process_image, flatten_json
except Exception:
    MAIN_AVAILABLE = False
    _import_error = traceback.format_exc()

# ---------- Folders ----------
INPUT_FOLDER = os.path.join("input", "uploads")
OUTPUT_PDF_IMAGES = os.path.join("output", "pdf_images")
OUTPUT_PROCESSED_IMAGES = os.path.join("output", "processed_images")
CLEANED_TEXT_FOLDER = "cleaned_text"

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs("output", exist_ok=True)
os.makedirs(OUTPUT_PDF_IMAGES, exist_ok=True)
os.makedirs(OUTPUT_PROCESSED_IMAGES, exist_ok=True)
os.makedirs(CLEANED_TEXT_FOLDER, exist_ok=True)

# ---------- Helpers ----------
def save_uploaded_files(uploaded_files):
    paths = []
    for up in uploaded_files:
        save_path = os.path.join(INPUT_FOLDER, up.name)
        with open(save_path, "wb") as f:
            f.write(up.getbuffer())
        paths.append(save_path)
    return paths

def process_permit(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        pdf_folder = os.path.dirname(file_path)
        image_output_folder = OUTPUT_PDF_IMAGES
        os.makedirs(image_output_folder, exist_ok=True)
        return process_pdf(os.path.basename(file_path), pdf_folder, image_output_folder)
    elif ext in [".png", ".jpg", ".jpeg"]:
        image_folder = os.path.dirname(file_path)
        image_output_folder = OUTPUT_PROCESSED_IMAGES
        os.makedirs(image_output_folder, exist_ok=True)
        return process_image(os.path.basename(file_path), image_folder, image_output_folder)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

def _file_sig(path):
    try:
        if not os.path.exists(path):
            return None
        stat = os.stat(path)
        if time.time() - stat.st_mtime < 0.5:
            time.sleep(0.1)
            stat = os.stat(path)
        return (stat.st_size, int(stat.st_mtime * 1000))
    except (FileNotFoundError, OSError):
        return None

# --- Validity helpers: guarantee "31-Dec-<year>" (no [unclear]) ---
def _extract_year(s: str) -> int | None:
    if not s:
        return None
    import re
    m = re.search(r"(19|20)\d{2}", s)
    return int(m.group(0)) if m else None

def _validity_31_dec(issue_date: str, validity_raw: str) -> str:
    # Prefer any explicit year from validity, else fall back to Issue_Date year, else current year
    y = _extract_year(validity_raw) or _extract_year(issue_date) or datetime.now().year
    return f"31-Dec-{y}"

# --- Titles helper (with legacy fallback) ---
def _collect_official_titles(data: dict) -> str:
    """Return semicolon-separated titles. Prefer parsed list, else parse legacy string."""
    titles = []

    # 1) From parsed list
    other_off = data.get("Other_Officials")
    if isinstance(other_off, list):
        for o in other_off:
            t = (o.get("title") or "").strip()
            if t:
                titles.append(t)

    # 2) Fallback: parse legacy "Other_Official_Names"
    if not titles:
        legacy = (data.get("Other_Official_Names") or "").strip()
        if legacy:
            parts = [p.strip() for p in legacy.split(";") if p.strip()]
            for p in parts:
                # Pattern A: "Name (Title)"
                if "(" in p and ")" in p and p.find("(") < p.find(")"):
                    t = p[p.find("(")+1:p.find(")")].strip()
                    if t:
                        titles.append(t)
                # Pattern B: "Name - Title"
                elif " - " in p:
                    _, title = p.split(" - ", 1)
                    t = title.strip()
                    if t:
                        titles.append(t)

    # De-dup while preserving order
    seen, deduped = set(), []
    for t in titles:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return "; ".join(deduped)

def excel_bytes_for_single_doc(data: dict) -> bytes:
    # UI field names with spaces -> underscores, in the requested order
    cols = [
        "Document_Type",
        "Page_Count",
        "Name_of_file",
        "Business_Name_Establishment",
        "Business_Owner",
        "Business_Address",
        "Mayor_Name",
        "Other_Official_Names",
        "Other_Official_Titles",
        "Municipality_City_Template",
        "Permit_Number",
        "Issue_Date",
        "Validity_Date",
        "Nature_of_Business",
        "raw_text",
        "cleaned_text",
    ]

    row = {c: "" for c in cols}
    row["Document_Type"] = data.get("Document_Type", "")
    row["Page_Count"] = data.get("Page_Count", "")
    row["Name_of_file"] = data.get("Name_of_file", "")

    # UI label: "Business Name/Establishment"
    row["Business_Name_Establishment"] = data.get("Business_Name", "")
    # UI label: "Business Owner"
    row["Business_Owner"] = data.get("Business_Owner_Name", "")
    # UI label: "Business Address"
    row["Business_Address"] = data.get("Business_Address", "")
    # UI label: "Mayor Name"
    row["Mayor_Name"] = data.get("Mayor_Name", "")
    # UI label: "Other Official Names"
    row["Other_Official_Names"] = data.get("Other_Official_Names", "")

    # Titles with literal 'None' rule
    names_str = str(row["Other_Official_Names"]).strip().lower()
    if names_str in ["none", "null", ""]:
        row["Other_Official_Titles"] = "None"
    else:
        row["Other_Official_Titles"] = _collect_official_titles(data) or "None"

    # UI label: "Municipality/City Template"
    row["Municipality_City_Template"] = data.get("Municipality_Template", data.get("Municipality_City", ""))
    # UI label: "Permit Number"
    row["Permit_Number"] = data.get("Permit_Number", "")
    # UI label: "Issue Date"
    row["Issue_Date"] = data.get("Issue_Date", "")
    # UI label: "Validity Date" (always 31-Dec-<year>)
    row["Validity_Date"] = _validity_31_dec(row["Issue_Date"], data.get("Business_Permit_Validity", ""))
    # UI label: "Nature of Business"
    row["Nature_of_Business"] = data.get("Business_Type", "")

    row["raw_text"] = data.get("raw_text", "")
    row["cleaned_text"] = data.get("cleaned_text", "")

    df = pd.DataFrame([row], columns=cols)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="extracted")
    buf.seek(0)
    return buf.read()


def excel_bytes_for_all_docs(cache: dict) -> bytes:
    cols = [
        "Document_Type",
        "Page_Count",
        "Name_of_file",
        "Business_Name_Establishment",
        "Business_Owner",
        "Business_Address",
        "Mayor_Name",
        "Other_Official_Names",
        "Other_Official_Titles",
        "Municipality_City_Template",
        "Permit_Number",
        "Issue_Date",
        "Validity_Date",
        "Nature_of_Business",
        "raw_text",
        "cleaned_text",
    ]
    rows = []
    for entry in cache.values():
        data = (entry or {}).get("result")
        if not data:
            continue

        row = {c: "" for c in cols}
        row["Document_Type"] = data.get("Document_Type", "")
        row["Page_Count"] = data.get("Page_Count", "")
        row["Name_of_file"] = data.get("Name_of_file", "")

        row["Business_Name_Establishment"] = data.get("Business_Name", "")
        row["Business_Owner"] = data.get("Business_Owner_Name", "")
        row["Business_Address"] = data.get("Business_Address", "")
        row["Mayor_Name"] = data.get("Mayor_Name", "")

        # Names & Titles (with literal 'None' rule)
        row["Other_Official_Names"] = data.get("Other_Official_Names", "")
        names_str = str(row["Other_Official_Names"]).strip().lower()
        if names_str in ["none", "null", ""]:
            row["Other_Official_Titles"] = "None"
        else:
            row["Other_Official_Titles"] = _collect_official_titles(data) or "None"

        row["Municipality_City_Template"] = data.get("Municipality_Template", data.get("Municipality_City", ""))
        row["Permit_Number"] = data.get("Permit_Number", "")
        row["Issue_Date"] = data.get("Issue_Date", "")
        row["Validity_Date"] = _validity_31_dec(row["Issue_Date"], data.get("Business_Permit_Validity", ""))
        row["Nature_of_Business"] = data.get("Business_Type", "")

        row["raw_text"] = data.get("raw_text", "")
        row["cleaned_text"] = data.get("cleaned_text", "")

        rows.append(row)

    df = pd.DataFrame(rows, columns=cols)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="extracted")
    buf.seek(0)
    return buf.read()

# ---------- Upload and Cache Management ----------
uploaded_files = st.file_uploader("", type=["pdf", "png", "jpg", "jpeg"], accept_multiple_files=True)

if "cache" not in st.session_state:
    st.session_state["cache"] = {}

if "selected_file_path" not in st.session_state:
    st.session_state["selected_file_path"] = None

if "uploaded_file_names" not in st.session_state:
    st.session_state["uploaded_file_names"] = set()

newly_uploaded = []
if uploaded_files:
    current_upload_names = set(f.name for f in uploaded_files)
    
    if current_upload_names != st.session_state["uploaded_file_names"]:
        saved_paths = save_uploaded_files(uploaded_files)
        newly_uploaded = saved_paths.copy()
        st.session_state["uploaded_file_names"] = current_upload_names
        st.success(f"Saved {len(saved_paths)} uploaded file(s) to `{INPUT_FOLDER}`")
        
        for path in newly_uploaded:
            if path in st.session_state["cache"]:
                del st.session_state["cache"][path]

if not MAIN_AVAILABLE:
    st.error("Error importing processing functions from main.py – processing disabled.")
    st.code(_import_error)
    st.stop()

all_files = sorted({os.path.join(INPUT_FOLDER, f) for f in os.listdir(INPUT_FOLDER) if os.path.isfile(os.path.join(INPUT_FOLDER, f))})
if not all_files:
    st.info("No files available. Upload a PDF or image above to get started.")
    st.stop()

def needs_processing(file_path):
    current_sig = _file_sig(file_path)
    if current_sig is None:
        return False
    
    cache_entry = st.session_state["cache"].get(file_path)
    if cache_entry is None:
        return True
    
    cached_sig = cache_entry.get("sig")
    if cached_sig != current_sig:
        return True
    
    if cache_entry.get("result") is None:
        return True
    
    return False

def batch_process(paths, force_process=False):
    if not paths:
        return
    
    progress_holder = st.empty()
    with st.spinner(f"Processing {len(paths)} document(s)…"):
        progress = progress_holder.progress(0, text="Starting…")
        completed, total = 0, len(paths)
        
        def process_single_file(p):
            try:
                return process_permit(p)
            except Exception as e:
                st.warning(f"Failed to process {os.path.basename(p)}: {e}")
                st.code(traceback.format_exc())
                return None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, total)) as ex:
            futures = {ex.submit(process_single_file, p): p for p in paths}
            
            for fut in concurrent.futures.as_completed(futures):
                p = futures[fut]
                res = fut.result()
                
                current_sig = _file_sig(p)
                st.session_state["cache"][p] = {
                    "sig": current_sig, 
                    "result": res,
                    "processed_at": time.time()
                }
                
                completed += 1
                progress.progress(int(completed/total*100), text=f"Processed {completed}/{total}")
                time.sleep(0.02)
        
        progress_holder.empty()

if newly_uploaded:
    time.sleep(0.3)
    
    verified_uploads = []
    for p in newly_uploaded:
        if os.path.exists(p) and os.path.getsize(p) > 0:
            verified_uploads.append(p)
        else:
            st.warning(f"File {os.path.basename(p)} may not have been saved correctly")
    newly_uploaded = verified_uploads

pending = []
for p in all_files:
    if p in newly_uploaded:
        pending.append(p)
    elif needs_processing(p):
        pending.append(p)

if pending:
    batch_process(pending)

total = len(all_files)
processed = sum(1 for p in all_files if st.session_state["cache"].get(p, {}).get("result") is not None)

with st.sidebar:
    st.title("📁 Document Library")
    st.divider()

    col1, col2 = st.columns([0.5, 15.5])
       
    with col1:
        st.markdown("", unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="sb-label"><b>Find document:</b></div>', unsafe_allow_html=True)
        q = st.text_input("", placeholder="Search by filename..", key="sb_search")

        if q:
            filtered_files = [p for p in all_files if q.lower() in os.path.basename(p).lower()]
            if filtered_files:
                display_files = filtered_files
            else:
                st.info(f"No matches for '{q}'")
                display_files = []
        else:
            display_files = all_files

        st.markdown('<div class="sb-group">', unsafe_allow_html=True)
        st.markdown('<div class="sb-label"><b>Select document:</b></div>', unsafe_allow_html=True)

        if display_files:
            selected_idx = st.radio(
                "",
                options=list(range(len(display_files))),
                format_func=lambda i: os.path.basename(display_files[i]),
                key="sb_file_select_idx",
            )
            selected_path = display_files[selected_idx]
            
            if st.session_state["selected_file_path"] != selected_path:
                st.session_state["selected_file_path"] = selected_path
                
        elif q:
            st.info("Try a different search term")
            selected_path = st.session_state.get("selected_file_path")
        else:
            st.info("No files available.")
            selected_path = st.session_state.get("selected_file_path")

        st.markdown('</div>', unsafe_allow_html=True)

        if selected_path:
            entry = st.session_state["cache"].get(selected_path)
            status_icon = "Processed" if (entry and entry.get("result")) else ("Not yet processed" if (entry and "result" in entry and entry.get("result") is None) else "Processing…")
            
            if os.path.exists(selected_path):
                stat = os.stat(selected_path)
                file_kind = "PDF" if selected_path.lower().endswith(".pdf") else "Image"
                size_kb = stat.st_size // 1024
                
                st.markdown(
                    f'<div class="sb-help">Status: {status_icon} • Type: {file_kind} • Size: {size_kb} KB</div>',
                    unsafe_allow_html=True
                )

        st.divider()

    all_excel = excel_bytes_for_all_docs(st.session_state["cache"])

    st.download_button(
        "Export All Data",
        data=all_excel,
        file_name="business_permits_extracted.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="sb_download_all",
    )

    # REMOVED per request:
    # if st.button("Refresh All", key="sb_reprocess"):
    #     st.session_state["cache"].clear()
    #     st.rerun()
    #
    # if st.button("Reset Cache", key="sb_clear_cache"):
    #     st.session_state["cache"].clear()
    #     st.rerun()

selected_path = st.session_state.get("selected_file_path")
result = st.session_state["cache"].get(selected_path, {}).get("result") if selected_path else None

st.divider()
col1, col2, col3 = st.columns([30, 1, 40])

with col1:
    st.subheader("Document Preview")
    if selected_path and os.path.exists(selected_path):
        tab_original, tab_processed = st.tabs(["Original Image", "Processed Image"])
        ext = os.path.splitext(selected_path)[1].lower()

        with tab_original:
            if ext == ".pdf":
                st.info("Original is a PDF. You can download and view the original.")
                st.download_button(
                    "⬇️ Download Original PDF",
                    data=open(selected_path, "rb"),
                    file_name=os.path.basename(selected_path),
                )
            else:
                try:
                    st.image(selected_path, use_container_width=True)
                except Exception:
                    st.write("Preview not available for this image type.")
                    st.download_button(
                        "⬇️ Download Original Image",
                        data=open(selected_path, "rb"),
                        file_name=os.path.basename(selected_path),
                    )

        with tab_processed:
            if ext == ".pdf":
                base = os.path.splitext(os.path.basename(selected_path))[0]
                page1_path = os.path.join(OUTPUT_PDF_IMAGES, f"{base}_page_1.png")
                if os.path.exists(page1_path):
                    st.image(page1_path, use_container_width=True)
                else:
                    st.info("Processed preview will appear here after processing.")
            else:
                base = os.path.splitext(os.path.basename(selected_path))[0]
                processed_path = os.path.join(OUTPUT_PROCESSED_IMAGES, f"{base}_processed.png")
                if os.path.exists(processed_path):
                    st.image(processed_path, use_container_width=True)
                else:
                    st.info("Processed preview will appear here after processing.")
    else:
        st.info("No file selected or file not found.")

with col2:
    st.markdown("", unsafe_allow_html=True)

with col3:
    st.subheader("Extracted Data")
    if not result:
        st.info("No extracted data yet. If you just uploaded, processing should complete shortly.")
    elif not selected_path:
        st.info("Please select a document from the sidebar.")
    else:
        tabs = st.tabs(["Business Permit Details", "Cleaned Text", "Raw Extracted Text"])
        file_key = os.path.basename(selected_path)

        with tabs[0]:
            business_name = st.text_input(
                "**Business Name/Establishment**", result.get("Business_Name", ""), key=f"{file_key}_business_name"
            )
            owner_name = st.text_input(
                "**Business Owner**", result.get("Business_Owner_Name", ""), key=f"{file_key}_owner_name",
                help="Individual name or business entity name"
            )
            business_address = st.text_area(
                "**Business Address**", result.get("Business_Address", ""), key=f"{file_key}_business_address",
                height=80
            )
            mayor_name = st.text_input(
                "**Mayor Name**", result.get("Mayor_Name", ""), key=f"{file_key}_mayor_name",
                help="Title is included if present (e.g., Atty.)"
            )

            if isinstance(result.get("Other_Officials"), list) and result.get("Other_Officials"):
                formatted_officials = "\n".join(
                    f"{o.get('name','').strip()} - {o.get('title','').strip()}".strip(" -")
                    for o in result["Other_Officials"]
                    if (o.get("name") or o.get("title"))
                )
            else:
                legacy = result.get("Other_Official_Names", "")
                parts = [p.strip() for p in legacy.split(";") if p.strip()]
                normalized = []
                for p in parts:
                    if "(" in p and ")" in p and p.find("(") < p.find(")"):
                        name = p[:p.find("(")].strip()
                        title = p[p.find("(")+1:p.find(")")].strip()
                        normalized.append(f"{name} - {title}" if title else name)
                    else:
                        normalized.append(p)
                formatted_officials = "\n".join(normalized)

            other_officials_text = st.text_area(
                "**Other Official Names**",
                formatted_officials,
                key=f"{file_key}_other_officials",
                height=150,
                help="One per line, formatted as: Name - Title (include titles like Atty., Engr.)"
            )

            municipality_template = st.text_input(
                "**Municipality/City Template**",
                result.get("Municipality_Template", result.get("Municipality_City", "")),
                key=f"{file_key}_municipal_template",
            )
            permit_number = st.text_input(
                "**Permit Number**", result.get("Permit_Number", ""), key=f"{file_key}_permit_number"
            )
            issue_date = st.text_input(
                "**Issue Date**",
                result.get("Issue_Date", ""),
                key=f"{file_key}_issue_date",
                help="Format: dd-mmm-yyyy (e.g., 15-Mar-2024)"
            )

            # UPDATED: validity always shows "31-Dec-<same year>" (never "[unclear]")
            validity_default = _validity_31_dec(result.get("Issue_Date", ""), result.get("Business_Permit_Validity", ""))
            validity_date = st.text_input(
                "**Validity Date**",
                validity_default,
                key=f"{file_key}_validity_date",
                help="Auto-set to 31-Dec-<year>."
            )

            official_positions = st.text_area(
                "**Nature of Business**",
                result.get("Business_Type", ""),
                key=f"{file_key}_official_positions",
            )

            bcol1, bcol2 = st.columns(2)
            with bcol1:
                if st.button("Update Record", key=f"{file_key}_update_record"):
                    parsed_officials, legacy_lines = [], []
                    for line in (other_officials_text or "").splitlines():
                        line = line.strip()
                        if not line: continue
                        if " - " in line:
                            name, title = line.split(" - ", 1)
                            parsed_officials.append({"name": name.strip(), "title": title.strip()})
                            legacy_lines.append(f"{name.strip()} ({title.strip()})" if title.strip() else name.strip())
                        else:
                            parsed_officials.append({"name": line, "title": ""})
                            legacy_lines.append(line)

                    updated = result.copy()
                    updated.update({
                        "Business_Name": business_name,
                        "Business_Owner_Name": owner_name,
                        "Business_Address": business_address,
                        "Mayor_Name": mayor_name,
                        "Other_Officials": parsed_officials,
                        "Other_Official_Names": "; ".join(legacy_lines) if legacy_lines else "None",
                        "Municipality_Template": municipality_template,
                        "Permit_Number": permit_number,
                        "Issue_Date": issue_date,
                        "Business_Permit_Validity": validity_date,  # remains internal, export maps it
                        "Business_Type": official_positions,
                        "Name_of_file": os.path.basename(selected_path),
                    })
                    st.session_state["cache"][selected_path] = {"sig": _file_sig(selected_path), "result": updated}
                    result = updated
                    st.success("Changes saved.")

            with bcol2:
                current = st.session_state["cache"].get(selected_path, {"result": result})["result"]
                excel_bytes = excel_bytes_for_single_doc(current)
                st.download_button(
                    "Export to Excel",
                    data=excel_bytes,
                    file_name=f"{os.path.splitext(os.path.basename(selected_path))[0]}_extracted.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"{file_key}_download_excel",
                )

        with tabs[1]:
            if result.get("cleaned_text"):
                st.text_area("Cleaned Text", result.get("cleaned_text", ""), height=300, key=f"{file_key}_cleaned_text")
                base = os.path.splitext(os.path.basename(selected_path))[0]
                cleaned_path = os.path.join(CLEANED_TEXT_FOLDER, f"{base}.txt")
                if os.path.exists(cleaned_path):
                    with open(cleaned_path, "rb") as f:
                        st.download_button("Export Cleaned Text", data=f, file_name=f"{base}.txt", mime="text/plain", key=f"{file_key}_dl_cleaned")
                else:
                    st.download_button("Download", data=(result.get("cleaned_text") or "").encode("utf-8"),
                                       file_name=f"{base}.txt", mime="text/plain", key=f"{file_key}_dl_cleaned_mem")
            else:
                st.info("No cleaned text available.")

        with tabs[2]:
            if result.get("raw_text"):
                st.text_area("Raw Extracted Text", result.get("raw_text", ""), height=300, key=f"{file_key}_raw_text")
                base = os.path.splitext(os.path.basename(selected_path))[0]
                st.download_button("Export Raw Extracted Text",
                                   data=(result.get("raw_text") or "").encode("utf-8"),
                                   file_name=f"{base}_raw.txt", mime="text/plain",
                                   key=f"{file_key}_dl_raw")
            else:
                st.info("No raw OCR text available.")
