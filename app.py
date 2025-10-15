from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import pandas as pd
import os
import shutil
from werkzeug.utils import secure_filename
import zipfile
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'your-secret-key-here-change-in-production'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['OUTPUT_FOLDER'] = 'output_splits'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
app.config['ALLOWED_EXTENSIONS'] = {'xlsx', 'xls', 'csv'}

# Create necessary folders
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

def split_file_by_column(file_path, column_name, output_folder):
    """
    Splits Excel/CSV file data by unique values in a given column.
    Returns the output folder path so it can be used for downloading.
    Appends data to existing files if they already exist.
    """
    os.makedirs(output_folder, exist_ok=True)
    file_ext = os.path.splitext(file_path)[1].lower()
    files_created = []

    if file_ext in [".xlsx", ".xls"]:
        all_sheets = pd.read_excel(file_path, sheet_name=None)
        unique_values = set()
        
        for df in all_sheets.values():
            if column_name in df.columns:
                unique_values.update(df[column_name].dropna().unique())

        for value in unique_values:
            safe_value = str(value).replace("/", "_").replace("\\", "_").replace(":", "_")
            output_file = os.path.join(output_folder, f"{safe_value}.xlsx")

            # Check if file already exists
            if os.path.exists(output_file):
                # Read existing data
                existing_sheets = pd.read_excel(output_file, sheet_name=None)
                
                # Append new data to existing sheets
                with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                    sheet_written = False
                    for sheet_name, df in all_sheets.items():
                        if column_name not in df.columns:
                            continue
                        filtered_df = df[df[column_name] == value]
                        
                        # If sheet exists in old file, append data
                        if sheet_name in existing_sheets and not filtered_df.empty:
                            combined_df = pd.concat([existing_sheets[sheet_name], filtered_df], ignore_index=True)
                            combined_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                            sheet_written = True
                        elif not filtered_df.empty:
                            filtered_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                            sheet_written = True
                        elif sheet_name in existing_sheets:
                            # Keep existing sheet even if no new data
                            existing_sheets[sheet_name].to_excel(writer, sheet_name=sheet_name[:31], index=False)
                            sheet_written = True
                    
                    if sheet_written:
                        files_created.append(f"{safe_value}.xlsx")
            else:
                # Create new file
                with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                    sheet_written = False
                    for sheet_name, df in all_sheets.items():
                        if column_name not in df.columns:
                            continue
                        filtered_df = df[df[column_name] == value]
                        if not filtered_df.empty:
                            filtered_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                            sheet_written = True

                if sheet_written:
                    files_created.append(f"{safe_value}.xlsx")
                else:
                    if os.path.exists(output_file):
                        os.remove(output_file)

    elif file_ext == ".csv":
        df = pd.read_csv(file_path)

        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in CSV.")

        for value, group in df.groupby(column_name):
            if group.empty:
                continue
            safe_value = str(value).replace("/", "_").replace("\\", "_").replace(":", "_")
            output_file = os.path.join(output_folder, f"{safe_value}.csv")
            
            # If file exists, append data
            if os.path.exists(output_file):
                existing_df = pd.read_csv(output_file)
                combined_df = pd.concat([existing_df, group], ignore_index=True)
                combined_df.to_csv(output_file, index=False)
            else:
                group.to_csv(output_file, index=False)
            
            files_created.append(f"{safe_value}.csv")

    return output_folder, files_created

def create_zip(folder_path, zip_name):
    """Create a zip file from a folder."""
    # Ensure output folder exists
    os.makedirs(app.config['OUTPUT_FOLDER'], exist_ok=True)
    
    # Create zip in the same directory as the script
    zip_path = os.path.abspath(f"{zip_name}.zip")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_path)
                zipf.write(file_path, arcname)
    return zip_path

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
        # Save uploaded file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_filename = f"{timestamp}_{filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(file_path)
        
        try:
            # Split the file
            output_folder = os.path.join(app.config['OUTPUT_FOLDER'], f"split_{timestamp}")
            output_folder, files_created = split_file_by_column(file_path, column_name, output_folder)
            
            if not files_created:
                flash(f'No data found for column "{column_name}"', 'error')
                return redirect(url_for('index'))
            
            # Create zip file
            zip_path = create_zip(output_folder, f"split_files_{timestamp}")
            
            # Check if zip file exists
            if not os.path.exists(zip_path):
                flash('Error creating zip file', 'error')
                return redirect(url_for('index'))
            
            flash(f'Successfully split into {len(files_created)} files!', 'success')
            
            # Send file and clean up after
            response = send_file(
                zip_path, 
                as_attachment=True, 
                download_name='split_files.zip',
                mimetype='application/zip'
            )
            
            # Schedule cleanup after response is sent
            @response.call_on_close
            def cleanup():
                try:
                    if os.path.exists(zip_path):
                        os.remove(zip_path)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    if os.path.exists(output_folder):
                        shutil.rmtree(output_folder)
                except Exception as e:
                    print(f"Cleanup error: {e}")
            
            return response
            
        except ValueError as e:
            flash(str(e), 'error')
            return redirect(url_for('index'))
        except Exception as e:
            flash(f'Error processing file: {str(e)}', 'error')
            return redirect(url_for('index'))
    
    else:
        flash('Invalid file type. Please upload Excel (.xlsx, .xls) or CSV (.csv) files only.', 'error')
        return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)