from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import pandas as pd
import os, shutil, zipfile, threading
from werkzeug.utils import secure_filename
from datetime import datetime
from time import sleep

app = Flask(__name__)
app.secret_key = 'your-secret-key-here-change-in-production'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'output_splits'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # allow 100 MB
app.config['ALLOWED_EXTENSIONS'] = {'xlsx', 'xls', 'csv'}

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


# ---------- Memory-safe splitter ----------
def split_large_csv(file_path, column_name, output_folder, chunk_size=2000):
    os.makedirs(output_folder, exist_ok=True)
    reader = pd.read_csv(file_path, chunksize=chunk_size)
    for chunk in reader:
        if column_name not in chunk.columns:
            raise ValueError(f"Column '{column_name}' not found in CSV.")
        for value, group in chunk.groupby(column_name):
            safe_value = str(value).replace("/", "_").replace("\\", "_").replace(":", "_")
            output_file = os.path.join(output_folder, f"{safe_value}.csv")
            mode = "a" if os.path.exists(output_file) else "w"
            header = not os.path.exists(output_file)
            group.to_csv(output_file, index=False, mode=mode, header=header)


def split_excel_sheets(file_path, column_name, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    all_sheets = pd.read_excel(file_path, sheet_name=None)
    unique_values = set()
    for df in all_sheets.values():
        if column_name in df.columns:
            unique_values.update(df[column_name].dropna().unique())

    for value in unique_values:
        safe_value = str(value).replace("/", "_").replace("\\", "_").replace(":", "_")
        output_file = os.path.join(output_folder, f"{safe_value}.xlsx")
        with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
            for sheet_name, df in all_sheets.items():
                if column_name not in df.columns:
                    continue
                filtered_df = df[df[column_name] == value]
                if not filtered_df.empty:
                    filtered_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)


def create_zip(folder_path, zip_name):
    zip_path = os.path.abspath(f"{zip_name}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)
    return zip_path


# ---------- Background worker ----------
def background_split(file_path, column_name, output_folder, zip_name):
    try:
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            split_large_csv(file_path, column_name, output_folder)
        else:
            split_excel_sheets(file_path, column_name, output_folder)
        zip_path = create_zip(output_folder, zip_name)
        print(f"[INFO] Split done: {zip_path}")
    except Exception as e:
        print(f"[ERROR] Split failed: {e}")


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file selected', 'error')
        return redirect(url_for('index'))

    file = request.files['file']
    column_name = request.form.get('column_name', '').strip()

    if not file.filename:
        flash('No file selected', 'error')
        return redirect(url_for('index'))
    if not column_name:
        flash('Please enter a column name', 'error')
        return redirect(url_for('index'))
    if not allowed_file(file.filename):
        flash('Invalid file type', 'error')
        return redirect(url_for('index'))

    # Save upload
    filename = secure_filename(file.filename)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_filename = f"{timestamp}_{filename}"
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
    file.save(file_path)

    # Output folder
    output_folder = os.path.join(app.config['OUTPUT_FOLDER'], f"split_{timestamp}")
    zip_name = f"split_files_{timestamp}"

    # Run split in background thread
    thread = threading.Thread(
        target=background_split, args=(file_path, column_name, output_folder, zip_name), daemon=True
    )
    thread.start()

    flash("✅ File uploaded! Processing in background. Please wait ~1-2 min and refresh /download page.", "success")
    return redirect(url_for('download_status', zip_name=zip_name))


@app.route('/download/<zip_name>')
def download_status(zip_name):
    """Check if the zip is ready and download it."""
    zip_path = os.path.abspath(f"{zip_name}.zip")
    if os.path.exists(zip_path):
        return send_file(zip_path, as_attachment=True, download_name="split_files.zip")
    else:
        return f"⏳ Still processing {zip_name}… refresh in a moment."


if __name__ == '__main__':
    app.run(debug=True, threaded=True)