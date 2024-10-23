# add_user.py
import bcrypt
import psycopg2
from config import get_db_connection

# Function to add a new user
def add_user(username, password):
    # Hash the password
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

    # Connect to the database
    conn = get_db_connection()
    cur = conn.cursor()

    # Insert the new user into the 'users' table
    cur.execute("INSERT INTO users (username, password_hash) VALUES (%s, %s)", (username, hashed_password.decode('utf-8')))

    # Commit the transaction
    conn.commit()
    cur.close()
    conn.close()
    print("User added successfully!")

if __name__ == '__main__':
    # Add the user
    username = 'analytics.growthpad@gmail.com'
    password = '@GDCSecData$$'
    add_user(username, password)
