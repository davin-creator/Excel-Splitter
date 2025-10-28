from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import pandas as pd
import os
import shutil
from werkzeug.utils import secure_filename
import zipfile
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'your-secret-key-here-change-in-production'

# --- Config ---
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'output_splits'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB upload limit
app.config['ALLOWED_EXTENSIONS'] = {'xlsx', 'xls', 'csv'}

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)


# --- Helpers ---
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']


def create_zip_stream(folder_path, zip_name):
    """Stream zip creation to avoid memory overload."""
    zip_path = os.path.abspath(f"{zip_name}.zip")
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)
    return zip_path


# --- Splitting Functions ---
def split_csv_by_column(file_path, column_name, output_folder):
    """Split large CSVs efficiently in chunks."""
    os.makedirs(output_folder, exist_ok=True)
    files_created = []
    try:
        for chunk in pd.read_csv(file_path, chunksize=10000):
            if column_name not in chunk.columns:
                raise ValueError(f"Column '{column_name}' not found in CSV.")
            for value, group in chunk.groupby(column_name):
                safe_value = str(value).replace("/", "_").replace("\\", "_").replace(":", "_")
                output_file = os.path.join(output_folder, f"{safe_value}.csv")
                mode = 'a' if os.path.exists(output_file) else 'w'
                header = not os.path.exists(output_file)
                group.to_csv(output_file, mode=mode, header=header, index=False)
                if output_file not in files_created:
                    files_created.append(f"{safe_value}.csv")
    except Exception as e:
        print("CSV split error:", e)
        raise
    return output_folder, files_created


def split_excel_by_column(file_path, column_name, output_folder):
    """Split Excel files safely for Render (no memory overflow, no book setter issue)."""
    os.makedirs(output_folder, exist_ok=True)
    files_created = []

    xls = pd.ExcelFile(file_path, engine="openpyxl")
    for sheet_name in xls.sheet_names:
        df = xls.parse(sheet_name)
        if column_name not in df.columns:
            continue

        for value, group in df.groupby(column_name):
            safe_value = str(value).replace("/", "_").replace("\\", "_").replace(":", "_")
            output_file = os.path.join(output_folder, f"{safe_value}.xlsx")

            # Use context manager - no manual writer.book manipulation
            if os.path.exists(output_file):
                with pd.ExcelWriter(output_file, engine="openpyxl", mode="a", if_sheet_exists="overlay") as writer:
                    group.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            else:
                with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
                    group.to_excel(writer, sheet_name=sheet_name[:31], index=False)

            if output_file not in files_created:
                files_created.append(f"{safe_value}.xlsx")

            # Free memory after each group
            del group

        # Free memory after each sheet
        del df

    return output_folder, files_created


# --- Routes ---
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

    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(url_for('index'))

    if not column_name:
        flash('Please enter a column name', 'error')
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_filename = f"{timestamp}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)

        output_folder = os.path.join(app.config['OUTPUT_FOLDER'], f"split_{timestamp}")
        os.makedirs(output_folder, exist_ok=True)

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                output_folder, files_created = split_csv_by_column(file_path, column_name, output_folder)
            else:
                output_folder, files_created = split_excel_by_column(file_path, column_name, output_folder)

            if not files_created:
                flash(f'No data found for column "{column_name}"', 'error')
                return redirect(url_for('index'))

            zip_path = create_zip_stream(output_folder, f"split_files_{timestamp}")

            response = send_file(
                zip_path,
                as_attachment=True,
                download_name='split_files.zip',
                mimetype='application/zip'
            )

            @response.call_on_close
            def cleanup():
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    if os.path.exists(output_folder):
                        shutil.rmtree(output_folder)
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
                except Exception as e:
                    print("Cleanup error:", e)

            return response

        except ValueError as e:
            flash(str(e), 'error')
            return redirect(url_for('index'))
        except Exception as e:
            print("Processing error:", e)
            flash(f'Error processing file: {str(e)}', 'error')
            return redirect(url_for('index'))

    else:
        flash('Invalid file type. Please upload Excel (.xlsx, .xls) or CSV (.csv) files only.', 'error')
        return redirect(url_for('index'))


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
