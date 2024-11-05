from flask import Blueprint, request, jsonify
import pandas as pd
from flask_jwt_extended import jwt_required
from config import get_db_connection

upload_bp = Blueprint('upload_bp', __name__)

@upload_bp.route('/api/upload', methods=['POST'])
@jwt_required()  # Ensure the user is authenticated
def upload_excel():
    if 'file' not in request.files:
        return jsonify({"msg": "No file part"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"msg": "No selected file"}), 400

    # Read the Excel file
    try:
        df = pd.read_excel(file, engine='openpyxl')

        # Strip whitespace from column headers
        df.columns = df.columns.str.strip()

        # Check if required columns exist
        required_columns = ['Website Name', 'URLs', 'Location']
        for col in required_columns:
            if col not in df.columns:
                return jsonify({"msg": f"Missing required column: {col}"}), 400

        website_names = df['Website Name'].tolist()
        urls = df['URLs'].tolist()
        locations = df['Location'].tolist()
    except Exception as e:
        return jsonify({"msg": "Error reading the Excel file", "error": str(e)}), 500

    # Store URLs in the database
    conn = get_db_connection()
    cur = conn.cursor()

    for website_name, url, location in zip(website_names, urls, locations):
        try:
            cur.execute(
                "INSERT INTO websites (name, url, location) VALUES (%s, %s, %s)",
                (website_name, url, location)
            )
        except Exception as e:
            conn.rollback()
            return jsonify({"msg": "Error adding URL", "error": str(e)}), 500

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"msg": "URLs added successfully"}), 201