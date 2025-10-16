# main.py - Updated export schema and validity handling (no other logic changed)
# - Removed "Municipality_City" from exported headers
# - Renamed columns in export:
#     Other_Official_Names -> Other_Official_Titles
#     Business_Permit_Validity -> Validity_Date
#     Business_Type -> Nature_of_Business
# - Ensured Validity_Date is always "31-Dec-<year>" (never "[unclear]") using validity year if present, else Issue_Date year, else current year

import concurrent.futures
import base64
import os
import requests
import csv
import json
from mimetypes import guess_type
from dotenv import load_dotenv
from pdf2image import convert_from_path
from datetime import datetime
import re
import cv2
import numpy as np
from PIL import Image
from io import BytesIO
import pandas as pd
import openpyxl
import time
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import io

load_dotenv()

# Azure OpenAI API endpoint and key
endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
api_key = os.getenv("AZURE_OPENAI_API_KEY")
if not endpoint or not api_key:
    raise ValueError("Azure OpenAI endpoint and API key must be set in .env file")

# Azure Document Intelligence API endpoint and key
adi_endpoint = os.getenv("ADI_ENDPOINT")
adi_api_key = os.getenv("ADI_API_KEY")
if not adi_endpoint or not adi_api_key:
    raise ValueError("Azure ADI endpoint and API key must be set in .env file")

headers = {
    "Content-Type": "application/json",
    "api-key": api_key
}

# --------------------- Image Preprocessing Functions ---------------------
def convert_pdf_to_images(pdf_path, output_folder):
    images = convert_from_path(pdf_path)
    image_paths = []
    for i, image in enumerate(images):
        image_path = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(pdf_path))[0]}_page_{i + 1}.png")
        image.save(image_path, "PNG")
        image_paths.append(image_path)
    return image_paths, len(images)

def preprocess_image(image):
    open_cv_image = np.array(image)
    open_cv_image = cv2.cvtColor(open_cv_image, cv2.COLOR_RGB2BGR)
    gray = cv2.cvtColor(open_cv_image, cv2.COLOR_BGR2GRAY)
    contours, _ = cv2.findContours(gray, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if contours:
        cnts_sorted = sorted(contours, key=lambda x: cv2.contourArea(x), reverse=True)
        cnt = cnts_sorted[0]
        x, y, w, h = cv2.boundingRect(cnt)
        gray = gray[y:y+h, x:x+w]
    _, thresh = cv2.threshold(gray, 200, 235, cv2.THRESH_BINARY)
    adaptive_thresh = cv2.adaptiveThreshold(
        thresh, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 21, 5
    )
    processed_image = Image.fromarray(adaptive_thresh)
    return processed_image

def convert_image_to_base64(image_path):
    mime_type, _ = guess_type(image_path)
    if mime_type is None:
        mime_type = "application/octet-stream"
    with open(image_path, "rb") as image_file:
        base64_encoded_data = base64.b64encode(image_file.read()).decode("utf-8")
    return f"data:{mime_type};base64,{base64_encoded_data}"

# Handle both PDF and image files
def process_image_file(image_path, output_folder):
    base_name = os.path.splitext(os.path.basename(image_path))[0]
    processed_image_path = os.path.join(output_folder, f"{base_name}_processed.png")
    image = Image.open(image_path)
    processed_image = preprocess_image(image)
    processed_image.save(processed_image_path)
    return [processed_image_path], 1

# Azure OCR API call to extract raw text from the image
def get_raw_text(image_data_url):
    try:
        client = DocumentAnalysisClient(endpoint=adi_endpoint, credential=AzureKeyCredential(adi_api_key))
        if image_data_url.startswith('data:'):
            header, base64_data = image_data_url.split(',', 1)
            image_bytes = base64.b64decode(base64_data)
            image_stream = io.BytesIO(image_bytes)
            poller = client.begin_analyze_document("prebuilt-document", image_stream)
            result = poller.result()
            extracted_text = " ".join([line.content for page in result.pages for line in page.lines])
            return extracted_text
        else:
            with open(image_data_url, "rb") as image_file:
                poller = client.begin_analyze_document("prebuilt-document", image_file)
                result = poller.result()
            extracted_text = result.content
            return extracted_text
    except Exception as e:
        print(f"Error analyzing document: {e}")
        import traceback
        traceback.print_exc()
        return None

#--------------------- Text Cleaning Functions ---------------------
def clean_ocr_text(raw_text, image):
    extracted_text = raw_text
    system_prompt = """
    You are an expert OCR text cleaner specializing in Philippine business permits. Your task is to clean and format the raw OCR text to make it more readable and easier to parse for name extraction and differentiation.

    Fix spacing and line breaks, correct obvious OCR errors, preserve structure, and do not add information. Output plain text only.
    """
    try:
        data = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": extracted_text},
                        {"type": "image_url", "image_url": {"url": image}},
                    ],
                }
            ],
            "max_tokens": 4000,
            "temperature": 0
        }
        response = requests.post(endpoint, headers=headers, json=data)
        response.raise_for_status()
        cleaned_text = response.json()["choices"][0]["message"]["content"]
        return cleaned_text
    except Exception as e:
        print(f"Error in OCR text cleaning: {str(e)}")
        return raw_text

# --------------------- Structured Data Functions ---------------------
def parse_structured_response(response_content):
    if isinstance(response_content, dict):
        return response_content
    if isinstance(response_content, str):
        json_match = re.search(r'<initial_attempt>\s*```json(.*?)```\s*</initial_attempt>', response_content, re.DOTALL)
        if json_match:
            json_str = json_match.group(1).strip()
            try:
                structured_data = json.loads(json_str)
                return structured_data
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                print("Extracted JSON was:", json_str)
                return None
        else:
            print("No JSON found in <initial_attempt> tags.")
            return None
    print("Unexpected response content type:", type(response_content))
    return None

def get_structured_data_from_text(raw_text):
    system_prompt = """
        You are an AI assistant specialized in extracting and differentiating names from Philippine business permits. Your primary goal is to demonstrate advanced AI capabilities in distinguishing between different types of names and entities mentioned in the document.

        <user_task>
        ═══════════════════════════════════════════════════════════════
        1. PURPOSE AND OUTPUT REQUIREMENTS
        ═══════════════════════════════════════════════════════════════
        1.1 Goal: Extract and differentiate names from Philippine business permits with absolute accuracy, focusing on the AI's ability to categorize different types of names and identify municipal/city templates.

        Key Objectives:
        • Demonstrate AI's capability to differentiate between individual names vs business names vs official names
        • Parse business permit documents and identify different name categories with context understanding
        • Identify municipal/city template variations
        • Extract supporting information like permit numbers, dates, and addresses
        • Showcase contextual understanding of Filipino naming conventions and business permit formats
        • Include professional titles (e.g., Atty., Engr., Dr.) with names when present

        1.2 Critical Requirements:
        • Extract ONLY the specified fields
        • Strict JSON format – no deviations
        • Missing fields must be explicitly labeled as "None"
        • Multi-page documents must be combined into a single structured JSON object
        • No assumptions or inferences: Only extract what is explicitly visible
        • PRIMARY FOCUS: Demonstrate NAME DIFFERENTIATION capabilities
        • Preserve exact spelling of Filipino names and business names
        • Identify municipal/city template types
        • Include titles (Atty., Engr., Dr., etc.) with names
        • ALL dates must be in dd-mmm-yyyy format (e.g., 15-Mar-2024, 01-Jan-2025)
        • NEVER infer or calculate dates from partial information

        ═══════════════════════════════════════════════════════════════
        2. TEMPLATE IDENTIFICATION
        ═══════════════════════════════════════════════════════════════
        Identify the municipal/city template from the following common types:
        • Manila City
        • Quezon City
        • Makati City
        • Cebu City
        • Davao City
        • Pasig City
        • Taguig City
        • Antipolo City
        • Dasmariñas City
        • Biñan City
        • Imus City
        • Cainta
        • Las Piñas City
        • Parañaque City
        • Muntinlupa City
        • Caloocan City
        • Marikina City
        • Pasay City
        • Valenzuela City
        • Malabon City
        • Navotas City
        • San Juan City
        • Mandaluyong City
        • Other Municipal Template
        • Unknown Template

        ═══════════════════════════════════════════════════════════════
        3. NAME DIFFERENTIATION AND EXTRACTION RULES (PRIMARY FOCUS)
        ═══════════════════════════════════════════════════════════════
        
        3.1 Business Owner Name (Individual or Business Entity):
        • Can be either:
        - Full name of the individual person who owns/operates the business (e.g., "Atty. Juan dela Cruz", "Maria Santos-Garcia")
        - OR the business/company name if the owner is a corporate entity (e.g., "ABC Corporation", "XYZ Enterprises, Inc.")
        • Usually found in "Applicant Name", "Owner", "Proprietor", "Pangalan ng May-ari" sections
        • Include professional titles when present (Atty., Engr., Dr., etc.)
        • May include middle names, maiden names, or compound surnames for individuals
        • Context: This demonstrates AI's ability to identify either individual human names OR business entity names as owners

        3.2 Mayor Name (Government Official):
        • Full name of the municipal mayor including title if present (e.g., "Atty. Juan dela Cruz")
        • Often found with official signatures, seals, or "Punong Lungsod/Bayan" designation
        • Extract the person's name with title
        • May appear in signature blocks or approval sections
        • Context: Shows AI can identify specific government official names with titles

        3.3 Business Name/Establishment:
        • Official registered name of the business establishment
        • Trade names, company names, store names (e.g., "Sari-sari Store ni Maria", "ABC General Merchandise")
        • May include business type descriptors (Store, Shop, Restaurant, etc.)
        • May be in English, Filipino, or mixed languages
        • Context: Demonstrates AI's ability to distinguish business entities from personal names

        3.4 Business Address:
        • Complete business address including street, barangay, city/municipality, and province if visible
        • Extract full address as stated in the permit
        • Include all address components visible in the document
        • Format: Street/Building, Barangay, City/Municipality, Province

        3.5 Other Official Names (Government/Municipal Officials):
        • Names of city/municipal officials mentioned in the document
        • Department heads, treasurers, assessors, clerks, witnesses
        • Business permit officers, licensing officers
        • Anyone with an official government title or position
        • Include professional titles (Atty., Engr., etc.) with names
        • List multiple names separated by semicolons if multiple officials are present
        • Context: Shows AI's contextual understanding of various official roles
        • Format: "Atty. Roberto Martinez (City Treasurer); Engr. Ana Reyes (Business Permit Officer)"

        3.6 Supporting Information:
        • Municipality/City Template: Specific template format used
        • Permit Number: Official permit/license number
        • Issue Date: Date when permit was issued (format: dd-mmm-yyyy)
        CRITICAL: Only extract if the COMPLETE date (day, month, year) is explicitly visible
        If incomplete, use "[unclear]"
        • Business Permit Validity: Validity/expiration date of permit (format: dd-mmm-yyyy)
        CRITICAL: Only extract if the COMPLETE date (day, month, year) is explicitly visible
        If incomplete (e.g., only year shown, or "quarter" without specific date), use "[unclear]"
        NEVER calculate or infer dates from partial information
        NEVER assume quarter end dates
        • Business Type: Type of business operation if clearly stated
        • Municipality/City: Full name of the issuing municipality/city

        ═══════════════════════════════════════════════════════════════
        4. OUTPUT FORMAT
        ═══════════════════════════════════════════════════════════════
        Produce a single JSON object containing exactly the following fields:

        {
            "Municipality_Template": "[Manila City|Quezon City|Makati City|Cebu City|Davao City|Pasig City|Taguig City|Antipolo City|Dasmariñas City|Biñan City|Imus City|Cainta|Las Piñas City|Parañaque City|Muntinlupa City|Caloocan City|Marikina City|Pasay City|Valenzuela City|Malabon City|Navotas City|San Juan City|Mandaluyong City|Other Municipal Template|Unknown Template]",
            "Document_Type": "Philippine Business Permit",
            "Page_Count": "integer",
            "Municipality_City": "string",
            "Business_Owner_Name": "string (individual name with title OR business entity name)",
            "Mayor_Name": "string (include title if present)", 
            "Business_Name": "string",
            "Business_Address": "string",
            "Other_Official_Names": "string (include titles)",
            "Permit_Number": "string",
            "Issue_Date": "string (dd-mmm-yyyy format, or [unclear] if incomplete)",
            "Business_Permit_Validity": "string (dd-mmm-yyyy format, or [unclear] if incomplete)",
            "Business_Type": "string"
        }

        Notes:  
        • Mark any field explicitly absent as "None"
        • If data is visible but unclear, use "[unclear]"
        • Ensure no extraneous keys are added
        • PRIMARY FOCUS: Accurate name differentiation to showcase AI capability
        • Use underscore format for field names to ensure Excel compatibility
        • Always include professional titles (Atty., Engr., Dr., etc.) with names when visible
        • Format ALL dates as dd-mmm-yyyy (e.g., 15-Mar-2024, 01-Jan-2025, 31-Dec-2024)

        ═══════════════════════════════════════════════════════════════
        5. DATE EXTRACTION RULES - STRICT COMPLIANCE REQUIRED
        ═══════════════════════════════════════════════════════════════

        For Issue_Date and Business_Permit_Validity fields:

        ONLY extract dates that are COMPLETELY and EXPLICITLY visible with ALL three components:
        • Full day number (01-31)
        • Full month name or abbreviation
        • Full year (4 digits)

        If ANY component is missing, unclear, or requires inference:
        • Return "[unclear]" 
        • DO NOT calculate quarter end dates
        • DO NOT infer missing day/month values
        • DO NOT assume dates from partial information
        • DO NOT convert "end of quarter" to specific dates

        Valid Examples:
        ✓ "December 31, 2018" → "31-Dec-2018"
        ✓ "15 March 2024" → "15-Mar-2024"
        ✓ "May 24, 2018" → "24-May-2018"
        
        Invalid Examples (use "[unclear]"):
        ✗ "End of 2018" → "[unclear]" (day/month missing)
        ✗ "Q3 2018" → "[unclear]" (specific date not visible)
        ✗ "___ QUARTER, 2018" → "[unclear]" (incomplete information)
        ✗ "VALID UNTIL THE END OF ___ QUARTER, 2018" → "[unclear]" (quarter not specified, date incomplete)
        ✗ "2018" → "[unclear]" (only year visible)
        ✗ "December 2018" → "[unclear]" (day missing)

        REMEMBER: When in doubt, use "[unclear]". Never guess or calculate dates.

        ═══════════════════════════════════════════════════════════════
        6. OUTPUT EXAMPLE (Demonstrating Name Differentiation)
        ═══════════════════════════════════════════════════════════════
        {
            "Municipality_Template": "Dasmariñas City",
            "Document_Type": "Philippine Business Permit",
            "Page_Count": "1",
            "Municipality_City": "Dasmariñas City, Cavite",
            "Business_Owner_Name": "Maria Santos-Cruz",
            "Mayor_Name": "Atty. Jennifer Austria Barzaga",
            "Business_Name": "Santos General Merchandise and Sari-sari Store",
            "Business_Address": "123 Main Street, Barangay Salitran, Dasmariñas City, Cavite",
            "Other_Official_Names": "Engr. Roberto Martinez (City Treasurer); Atty. Ana Reyes (Business Permit Officer); Jose Garcia (Department Head)",
            "Permit_Number": "BP-2024-001234",
            "Issue_Date": "15-Mar-2024",
            "Business_Permit_Validity": "31-Dec-2024",
            "Business_Type": "General Merchandise"
        }

        ═══════════════════════════════════════════════════════════════
        7. CRITICAL NOTES FOR NAME DIFFERENTIATION DEMONSTRATION
        ═══════════════════════════════════════════════════════════════
        • Individual vs Entity Recognition: The AI must clearly distinguish between personal names (individuals) and business entity names
        • Business Owner can be EITHER an individual person OR a business/corporate entity
        • Contextual Understanding: Use document structure, Filipino naming conventions, and official titles to aid proper categorization
        • Multiple Name Handling: When multiple officials are mentioned, demonstrate the ability to list and categorize them appropriately
        • Cultural Sensitivity: Preserve Filipino naming conventions including compound surnames, maiden names, and traditional naming patterns
        • Template Recognition: Identify different municipal templates to show document format understanding
        • Title Inclusion: Always include professional titles (Atty., Engr., Dr., etc.) when present in the document
        • Date Extraction: ONLY extract complete dates. Use "[unclear]" for any incomplete date information
        • Address Extraction: Extract complete business address with all visible components
        • The PRIMARY SUCCESS METRIC is the AI's demonstrated ability to correctly differentiate between different types of names based on context

        </user_task>

        Please follow these steps:

        1. Initial Attempt:
        Make an initial attempt at completing the task focusing on name differentiation. Present this attempt in <initial_attempt> tags with JSON format.

        2. Final Answer:
        Present your final JSON answer in <answer> tags after analysis.

    """
    try:
        data = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": [
                    {"type": "text", "text": "Extract and structure the information from the following Philippine business permit text. Provide your response in JSON format wrapped within ```json and ``` inside <initial_attempt> tags."},
                    {"type": "text", "text": raw_text}
                ]}
            ],
            "max_tokens": 8192,
            "temperature": 0.0
        }
        response = requests.post(endpoint, headers=headers, json=data)
        response.raise_for_status()
        response_content = response.json()["choices"][0]["message"]["content"]
        structured_data = parse_structured_response(response_content)
        return structured_data
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

def standardize_date(date_str):
    """(unchanged) Convert date to dd-mmm-yyyy format"""
    if not date_str or date_str in ["missing", "[unclear]"]:
        return date_str
    date_str = date_str.strip().replace("-", "/").replace(".", "/")
    patterns = [
        ('%Y/%m/%d', None),
        ('%d/%m/%Y', None),
        ('%m/%d/%Y', None),
        ('%Y-%m-%d', None),
        ('%d-%m-%Y', None),
        ('%m-%d-%Y', None),
    ]
    for pattern, _ in patterns:
        try:
            dt = datetime.strptime(date_str.replace("/", "-"), pattern.replace("/", "-"))
            return dt.strftime('%d-%b-%Y')
        except ValueError:
            continue
    return "[unclear]"

def merge_json_objects(json_objects, page_count):
    if not json_objects:
        return None
    merged_data = json_objects[0]
    merged_data["Page_Count"] = page_count
    for i in range(1, len(json_objects)):
        current_obj = json_objects[i]
        for key, value in current_obj.items():
            if key != "Page_Count":
                if merged_data[key] in ["missing", "[unclear]"] and value not in ["missing", "[unclear]"]:
                    merged_data[key] = value
                elif merged_data[key] != value and value not in ["missing", "[unclear]"]:
                    if "Name" in key and merged_data[key] not in ["missing", "[unclear]"]:
                        merged_data[key] = f"{merged_data[key]} / {value}"
    return merged_data

def flatten_json(nested_json):
    flat = {}
    for key, value in nested_json.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                flat[subkey] = subvalue
        else:
            flat[key] = value
    return flat

# NEW: helpers for export mapping
def _extract_year(s: str | None) -> int | None:
    if not s:
        return None
    m = re.search(r"(19|20)\d{2}", s)
    return int(m.group(0)) if m else None

def _validity_31_dec(issue_date: str | None, validity_raw: str | None) -> str:
    y = _extract_year(validity_raw) or _extract_year(issue_date) or datetime.now().year
    return f"31-Dec-{y}"

def save_to_excel(structured_data_list, excel_output_path):
    # Column names mirror UI labels with spaces -> underscores, in the exact order requested
    csv_headers = [
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

    flat_data_list = []
    for item in structured_data_list:
        flat = flatten_json(item)

        # Map UI-aligned columns
        flat["Business_Name_Establishment"] = flat.get("Business_Name")
        flat["Business_Owner"] = flat.get("Business_Owner_Name")
        flat["Other_Official_Names"] = flat.get("Other_Official_Names", "")
        # Titles (derive from structured list if present)
        other_off = flat.get("Other_Officials")
        if isinstance(other_off, (list, dict)):
            try:
                # ensure JSON string for persistence, but "Titles" column expects readable list (semicolon-separated)
                titles_list = []
                if isinstance(other_off, list):
                    titles_list = [ (o.get("title") or "").strip() for o in other_off if (o.get("title") or "").strip() ]
                flat["Other_Official_Titles"] = "; ".join(titles_list)
                flat["Other_Officials"] = json.dumps(other_off, ensure_ascii=False)
            except Exception:
                flat["Other_Official_Titles"] = ""
        else:
            flat["Other_Official_Titles"] = flat.get("Other_Official_Titles", "")

        # Municipality/City Template
        flat["Municipality_City_Template"] = flat.get("Municipality_Template") or flat.get("Municipality_City") or ""

        # Nature of Business & Validity Date (computed)
        flat["Nature_of_Business"] = flat.get("Business_Type")
        flat["Validity_Date"] = _validity_31_dec(flat.get("Issue_Date"), flat.get("Business_Permit_Validity"))

        flat_data_list.append(flat)

    df = pd.DataFrame(flat_data_list)
    for col in csv_headers:
        if col not in df.columns:
            df[col] = None
    df = df[csv_headers]
    df.to_excel(excel_output_path, index=False)
    print(f"Excel file saved to: {excel_output_path}")


# --------- Helper: derive structured officials list from cleaned text or legacy string ----------
ROLE_HINTS = [
    "officer", "treasurer", "licensing", "assessor", "clerk", "mayor",
    "head", "chief", "engineer", "inspector", "secretary", "administrator",
    "department", "director", "superintendent", "auditor", "witness", "atty", "engr"
]

def derive_official_pairs(structured_data, cleaned_text):
    pairs = []
    legacy = (structured_data or {}).get("Other_Official_Names") or ""
    if legacy:
        parts = [p.strip() for p in legacy.split(";") if p.strip()]
        for p in parts:
            if "(" in p and ")" in p and p.find("(") < p.find(")"):
                name = p[:p.find("(")].strip()
                title = p[p.find("(")+1:p.find(")")].strip()
                pairs.append({"name": name, "title": title})
            elif " - " in p:
                name, title = p.split(" - ", 1)
                pairs.append({"name": name.strip(), "title": title.strip()})
            else:
                pairs.append({"name": p, "title": ""})
    if not pairs and cleaned_text:
        lines = [ln.strip() for ln in cleaned_text.splitlines() if ln.strip()]
        for i in range(len(lines) - 1):
            nm, nxt = lines[i], lines[i+1]
            if re.search(r"[A-Za-z]\s+[A-Za-z]", nm) and any(h in nxt.lower() for h in ROLE_HINTS):
                pairs.append({"name": nm, "title": nxt})
    return pairs

# --------------------- PDF/Image processing ---------------------
def process_pdf(pdf_file, pdf_folder, image_folder):
    pdf_path = os.path.join(pdf_folder, pdf_file)
    print(f"Processing PDF: {pdf_file}...")
    image_paths, page_count = convert_pdf_to_images(pdf_path, image_folder)

    ocr_responses = []
    base64_data = None
    for image_path in image_paths:
        image = Image.open(image_path)
        processed_image = preprocess_image(image)
        processed_image.save(image_path)

        base64_data = convert_image_to_base64(image_path)
        raw_text_image = get_raw_text(base64_data)
        if raw_text_image:
            ocr_responses.append(raw_text_image)

    raw_text = "\n".join(ocr_responses)
    cleaned_text = clean_ocr_text(raw_text, base64_data)

    os.makedirs('cleaned_text', exist_ok=True)
    with open(f'cleaned_text/{pdf_file.replace(".pdf", "")}.txt', 'w', encoding='utf-8') as file:
        file.write(cleaned_text)

    structured_api_response = get_structured_data_from_text(cleaned_text)
    structured_data = structured_api_response or {}

    if structured_data:
        structured_data["Name_of_file"] = pdf_file
        structured_data["Page_Count"] = page_count
        structured_data["raw_text"] = raw_text
        structured_data["cleaned_text"] = cleaned_text
        structured_data["Other_Officials"] = derive_official_pairs(structured_data, cleaned_text)

    return structured_data

def process_image(image_file, image_input_folder, image_output_folder):
    image_path = os.path.join(image_input_folder, image_file)
    print(f"Processing Image: {image_file}...")
    image_paths, page_count = process_image_file(image_path, image_output_folder)

    ocr_responses = []
    base64_data = None
    for processed_image_path in image_paths:
        base64_data = convert_image_to_base64(processed_image_path)
        raw_text_image = get_raw_text(base64_data)
        if raw_text_image:
            ocr_responses.append(raw_text_image)

    raw_text = "\n".join(ocr_responses)
    cleaned_text = clean_ocr_text(raw_text, base64_data)

    os.makedirs('cleaned_text', exist_ok=True)
    base_name = os.path.splitext(image_file)[0]
    with open(f'cleaned_text/{base_name}.txt', 'w', encoding='utf-8') as file:
        file.write(cleaned_text)

    structured_api_response = get_structured_data_from_text(cleaned_text)
    structured_data = structured_api_response or {}

    if structured_data:
        structured_data["Name_of_file"] = image_file
        structured_data["Page_Count"] = page_count
        structured_data["raw_text"] = raw_text
        structured_data["cleaned_text"] = cleaned_text
        structured_data["Other_Officials"] = derive_official_pairs(structured_data, cleaned_text)

    return structured_data

# --------- CLI entry (optional local run) ---------
def main():
    pdf_folder = r"C:\path\to\input\pdfs"
    image_input_folder = r"C:\path\to\input\images"
    pdf_image_output_folder = r"C:\path\to\output\pdf_images"
    image_output_folder = r"C:\path\to\output\processed_images"
    excel_output = r"C:\path\to\output\business_permit_names_extracted.xlsx"

    os.makedirs(pdf_image_output_folder, exist_ok=True)
    os.makedirs(image_output_folder, exist_ok=True)
    os.makedirs(os.path.dirname(excel_output), exist_ok=True)
    os.makedirs('cleaned_text', exist_ok=True)

    structured_data_list = []
    pdf_files = [f for f in os.listdir(pdf_folder)] if os.path.exists(pdf_folder) else []
    image_files = [f for f in os.listdir(image_input_folder)] if os.path.exists(image_input_folder) else []

    if pdf_files:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_pdf, pdf_file, pdf_folder, pdf_image_output_folder): pdf_file for pdf_file in pdf_files}
            for future in concurrent.futures.as_completed(futures):
                pdf_file = futures[future]
                try:
                    structured_data = future.result()
                    if structured_data:
                        structured_data_list.append(structured_data)
                except Exception as exc:
                    print(f"{pdf_file} generated an exception: {exc}")

    if image_files:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = {executor.submit(process_image, image_file, image_input_folder, image_output_folder): image_file for image_file in image_files}
            for future in concurrent.futures.as_completed(futures):
                image_file = futures[future]
                try:
                    structured_data = future.result()
                    if structured_data:
                        structured_data_list.append(structured_data)
                except Exception as exc:
                    print(f"{image_file} generated an exception: {exc}")

    if structured_data_list:
        save_to_excel(structured_data_list, excel_output)
    else:
        print("No structured data extracted.")

def process_permit(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        pdf_folder = os.path.dirname(file_path)
        image_output_folder = os.path.join("output", "pdf_images")
        os.makedirs(image_output_folder, exist_ok=True)
        return process_pdf(os.path.basename(file_path), pdf_folder, image_output_folder)
    elif ext in [".jpg", ".jpeg", ".png"]:
        image_folder = os.path.dirname(file_path)
        image_output_folder = os.path.join("output", "processed_images")
        os.makedirs(image_output_folder, exist_ok=True)
        return process_image(os.path.basename(file_path), image_folder, image_output_folder)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Script generated an exception: {exc}")
