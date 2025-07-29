import os
import json
import pandas as pd
import xml.etree.ElementTree as ET
import re
import requests
import time
from flask import Flask, render_template, request, send_from_directory, flash, redirect, url_for
from werkzeug.utils import secure_filename
import yaml

# --- Configuration ---
# IMPORTANT: You must get your own API key from Google AI Studio.
# https://aistudio.google.com/app/apikey
with open("key.yaml", "r") as f:
    config = yaml.safe_load(f)
GEMINI_API_KEY = config.get("GEMINI_API_KEY")
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
PROCESSED_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'processed')
# --- UPDATED: Added 'csv' to allowed extensions ---
ALLOWED_EXTENSIONS = {'txt', 'log', 'json', 'xml', 'csv'}

# --- App Initialization ---
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
app.config['SECRET_KEY'] = 'supersecretkey'

# --- Helper Functions ---

def allowed_file(filename):
    """Checks if the file's extension is in the ALLOWED_EXTENSIONS set."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_regex_from_gemini(log_sample):
    """
    Sends a sample of the log file to the Gemini API to determine a parsing regex.
    This version asks for the raw regex string directly to avoid JSON formatting errors.
    """
    if GEMINI_API_KEY == "YOUR_API_KEY_HERE":
        raise ValueError("Gemini API key is not configured. Please set it in your environment or in the script.")

    # --- UPDATED PROMPT ---
    # Asks for the raw regex string, not a JSON object. This is more robust.
    prompt = f"""
    Analyze the following log data sample. Your task is to generate a single Python-compatible regular expression (regex) that can capture the distinct columns.

    Data Sample:
    ---
    {log_sample}
    ---

    Instructions:
    1.  Create a Python regex string with named capture groups (e.g., `?P<group_name>...`). The group names should be concise, descriptive, and in snake_case. These names will become the column headers.
    2.  **CRITICAL RULE:** If a timestamp (containing date, time, or both) is present, you MUST capture the entire timestamp in a single group named `timestamp`. Do NOT split it into multiple columns.
    3.  The regex MUST account for variable content, like messages that can contain spaces. Use non-greedy matching (`.*?`) for such fields.
    4.  The regex should match the entire line from start (`^`) to end (`$`).
    5.  **Return ONLY the raw regex string and absolutely nothing else.** Do not wrap it in quotes, markdown, or JSON.

    Example output for a log like "2025-07-27 19:56:34 [INFO] Quality check passed Machine-007 Operator-07":
    ^(?P<timestamp>\\d{{4}}-\\d{{2}}-\\d{{2}} \\d{{2}}:\\d{{2}}:\\d{{2}}) \\[(?P<log_level>\\w+)\\] (?P<message>.*?) (?P<machine_id>Machine-\\d{{3}}) (?P<operator_id>Operator-\\d{{2}})$
    """

    payload = {
        "contents": [{"parts": [{"text": prompt}]}]
    }
    
    headers = {'Content-Type': 'application/json'}
    
    # --- Retry Logic ---
    retries = 3
    backoff_factor = 1
    for i in range(retries):
        try:
            response = requests.post(GEMINI_API_URL, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if e.response is not None and e.response.status_code == 503 and i < retries - 1:
                print(f"Service unavailable, retrying in {backoff_factor} seconds...")
                time.sleep(backoff_factor)
                backoff_factor *= 2
                continue
            else:
                raise e
    
    raise requests.exceptions.RequestException("Failed to get a response from the API after several retries.")


# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload page."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the file upload and processing logic."""
    # --- FIXED: Redirect to 'index' on error ---
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('index'))
    
    file = request.files['file']

    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(upload_path)

        try:
            ext = filename.rsplit('.', 1)[1].lower()
            df = None
            
            if ext in ['log', 'txt']:
                with open(upload_path, 'r', encoding='utf-8', errors='ignore') as f:
                    log_sample = "".join(f.readlines(10))
                
                if not log_sample:
                    flash("File is empty.")
                    return redirect(url_for('index'))

                # --- NEW ROBUST PARSING ---
                gemini_response = get_regex_from_gemini(log_sample)
                
                # Safely extract the text from the API response
                regex_pattern = gemini_response.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text')

                if not regex_pattern:
                    raise ValueError("LLM did not return a regex pattern.")

                # Clean up potential markdown formatting from the response
                if regex_pattern.strip().startswith("```"):
                    regex_pattern = re.sub(r'```(python)?\n', '', regex_pattern)
                    regex_pattern = regex_pattern.strip().replace('```', '')

                print("\n--- Generated Regex Pattern ---")
                print(regex_pattern.strip())
                print("-----------------------------\n")

                pattern = re.compile(regex_pattern.strip())
                parsed_data = []
                with open(upload_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        match = pattern.match(line.strip())
                        if match:
                            parsed_data.append(match.groupdict())
                
                df = pd.DataFrame(parsed_data)

            elif ext == 'json':
                try:
                    with open(upload_path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    df = pd.json_normalize(json_data)
                except (json.JSONDecodeError, TypeError):
                    df = pd.read_json(upload_path, lines=True, encoding='utf-8')

            elif ext == 'xml':
                tree = ET.parse(upload_path)
                root = tree.getroot()
                records = [{child.tag: child.text for child in elem} for elem in root]
                df = pd.DataFrame(records)
            
            # --- NEW: Added direct CSV handling ---
            elif ext == 'csv':
                df = pd.read_csv(upload_path)


            if df is None or df.empty:
                flash('Could not parse the file. The format might be unsupported or the file is empty.')
                return redirect(url_for('index'))

            base_filename = filename.rsplit('.', 1)[0]
            csv_filename = f"{base_filename}_processed.csv" # Suffix to avoid name collision
            csv_path = os.path.join(app.config['PROCESSED_FOLDER'], csv_filename)
            df.to_csv(csv_path, index=False)
            
            preview_df = pd.read_csv(csv_path)
            preview_limit = 100
            preview_headers = preview_df.columns.values.tolist()
            rows = preview_df.head(preview_limit).values.tolist()
            
            return render_template('results.html', 
                                   csv_filename=csv_filename,
                                   headers=preview_headers,
                                   rows=rows)

        except Exception as e:
            flash(f'An error occurred: {e}')
            return redirect(url_for('index'))

    else:
        # --- FIXED: Redirect to 'index' on error ---
        flash('File type not allowed.')
        return redirect(url_for('index'))

@app.route('/download/<filename>')
def download_file(filename):
    """Serves the processed CSV file for download."""
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename, as_attachment=True)

# --- Main Execution ---
if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)
    app.run(debug=True)
