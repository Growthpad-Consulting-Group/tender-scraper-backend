from flask import Blueprint, request, jsonify, current_app
from app.config import get_db_connection
from flask_jwt_extended import jwt_required
from app.cache.redis_cache import set_cache, get_cache, delete_cache

# Create a Blueprint for countries management
countries_bp = Blueprint('countries_bp', __name__)

# Get All Countries
@countries_bp.route('/api/countries', methods=['GET'])
@jwt_required()
def get_countries():
    current_app.logger.debug("Received request to GET countries")
    cache_key = 'countries_list'

    # Attempt to retrieve the countries from cache
    cached_countries = get_cache(cache_key)
    if cached_countries:
        current_app.logger.debug("Retrieved countries from cache.")
        return jsonify(cached_countries), 200

    # If not in cache, retrieve from the database
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM countries")
            countries = cur.fetchall()

            # Check if any countries were returned
            if not countries:
                current_app.logger.debug("No countries found in the database.")
                return jsonify([]), 200  # Return an empty list if no countries are found

            # Adjust mapping to account for lists instead of tuples
            countries_list = [
                {"country_name": country[0], "iso_code": country[1]} # We're assuming that each country entry has two elements
                for country in countries
                if len(country) >= 2   # Ensure that country has at least 2 items
            ]

            # Check if countries_list is empty after processing
            if not countries_list:
                current_app.logger.debug("No valid country records found after processing.")
                return jsonify([]), 200  # Again, return empty if no valid entries

            # Store the countries in cache for future requests
            set_cache(cache_key, countries_list)

            current_app.logger.debug("Countries retrieved from database and cached.")
            return jsonify(countries_list), 200
    except Exception as e:
        current_app.logger.error(f"Failed to retrieve countries: {str(e)}")
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()



# Add a Country
@countries_bp.route('/api/countries', methods=['POST'])
@jwt_required()
def add_country():
    """Adds a new country to the database."""
    data = request.json
    country_name = data.get('country_name')
    iso_code = data.get('iso_code')

    if not country_name or not iso_code:
        return jsonify({"msg": "Country name and ISO code are required"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO countries (country_name, iso_code) VALUES (%s, %s)", (country_name, iso_code))
            conn.commit()
            delete_cache('countries_list')  # Invalidate cache
            return jsonify({"msg": "Country added", "country_name": country_name, "iso_code": iso_code}), 201
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Edit Country
@countries_bp.route('/api/countries/<int:country_id>', methods=['PUT'])
@jwt_required()
def edit_country(country_id):
    """Updates an existing country in the database."""
    data = request.json
    country_name = data.get('country_name')
    iso_code = data.get('iso_code')

    if not country_name and not iso_code:
        return jsonify({"msg": "Country name or ISO code must be provided"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if country_name:
                cur.execute("UPDATE countries SET country_name = %s WHERE id = %s", (country_name, country_id))
            if iso_code:
                cur.execute("UPDATE countries SET iso_code = %s WHERE id = %s", (iso_code, country_id))
            conn.commit()
            delete_cache('countries_list')  # Invalidate cache
            return jsonify({"msg": "Country updated", "country_id": country_id}), 200
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()

# Delete Country
@countries_bp.route('/api/countries/<int:country_id>', methods=['DELETE'])
@jwt_required()
def delete_country(country_id):
    """Deletes a country from the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM countries WHERE id = %s", (country_id,))
            conn.commit()
            delete_cache('countries_list')  # Invalidate cache
            return jsonify({"msg": "Country deleted"}), 200
    except Exception as e:
        return jsonify({"msg": str(e)}), 400
    finally:
        conn.close()