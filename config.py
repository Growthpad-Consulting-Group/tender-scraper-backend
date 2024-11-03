import pg8000

def get_db_connection():
    connection = pg8000.connect(
        host="aws-0-ap-south-1.pooler.supabase.com",
        database="postgres",
        user="postgres.wvbrwhnvvcpmlbuugumm",
        password="@Kwantw3Fo12",
        port=5432
    )
    return connection
