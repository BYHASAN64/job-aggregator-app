import sqlite3
from datetime import datetime

def connect_db(db_name="jobs.db"):

    conn = sqlite3.connect(db_name)

    # Access rows like dictionary
    conn.row_factory = sqlite3.Row

    cursor = conn.cursor()

    # Required for foreign/triggers stability
    cursor.execute("PRAGMA journal_mode=WAL")

    return conn, cursor

def create_tables(cursor):

    # -------- MAIN JOB TABLE --------

    cursor.execute("""

    CREATE TABLE IF NOT EXISTS jobs (

        id INTEGER PRIMARY KEY AUTOINCREMENT,

        source TEXT NOT NULL,

        category TEXT NOT NULL,

        title TEXT NOT NULL,

        company TEXT NOT NULL,

        location TEXT,

        job_url TEXT UNIQUE,

        first_seen TEXT,

        last_seen TEXT,

        is_active INTEGER DEFAULT 1

    )

    """)


    # -------- INDEXES --------

    cursor.execute("""

    CREATE INDEX IF NOT EXISTS idx_job_url
    ON jobs(job_url)

    """)

    cursor.execute("""

    CREATE INDEX IF NOT EXISTS idx_category
    ON jobs(category)

    """)

    cursor.execute("""

    CREATE INDEX IF NOT EXISTS idx_source_category
    ON jobs(source, category)

    """)

    cursor.execute("""

    CREATE INDEX IF NOT EXISTS idx_active
    ON jobs(is_active)

    """)


    # ------------------------------
    # FULL TEXT SEARCH TABLE
    # ------------------------------

    cursor.execute("""

    CREATE VIRTUAL TABLE IF NOT EXISTS jobs_fts
    USING fts5(

        title,
        company,
        category,
        location,
        job_url,

        content='jobs',
        content_rowid='id'

    )

    """)


    # ------------------------------
    # TRIGGERS (AUTO SYNC FTS)
    # ------------------------------

    # INSERT

    cursor.execute("""

    CREATE TRIGGER IF NOT EXISTS jobs_ai
    AFTER INSERT ON jobs BEGIN

        INSERT INTO jobs_fts
        (rowid,title,company,category,location,job_url)

        VALUES(

            new.id,
            new.title,
            new.company,
            new.category,
            new.location,
            new.job_url

        );

    END;

    """)


    # DELETE

    cursor.execute("""

    CREATE TRIGGER IF NOT EXISTS jobs_ad
    AFTER DELETE ON jobs BEGIN

        INSERT INTO jobs_fts(jobs_fts,rowid)
        VALUES('delete',old.id);

    END;

    """)


    # UPDATE

    cursor.execute("""

    CREATE TRIGGER IF NOT EXISTS jobs_au
    AFTER UPDATE ON jobs BEGIN

        INSERT INTO jobs_fts(jobs_fts,rowid)
        VALUES('delete',old.id);

        INSERT INTO jobs_fts
        (rowid,title,company,category,location,job_url)

        VALUES(

            new.id,
            new.title,
            new.company,
            new.category,
            new.location,
            new.job_url

        );

    END;

    """)


# ------------------------------
# VALIDATION PIPELINE
# ------------------------------

def validate_job(job):

    required = [

        "title",
        "company",
        "job_url",
        "category",
        "source"

    ]

    for field in required:

        if not job.get(field):

            return False

    return True


# ------------------------------
# INSERT OR UPDATE
# ------------------------------

def insert_or_update_job(

        cursor,
        source,
        category,
        title,
        company,
        location,
        job_url

):

    now = datetime.utcnow().isoformat()


    cursor.execute("""

    INSERT INTO jobs (

        source,
        category,
        title,
        company,
        location,
        job_url,
        first_seen,
        last_seen

    )

    VALUES (?, ?, ?, ?, ?, ?, ?, ?)

    ON CONFLICT(job_url)

    DO UPDATE SET

        title = excluded.title,
        company = excluded.company,
        location = excluded.location,
        category = excluded.category,
        last_seen = excluded.last_seen,
        is_active = 1

    """,

    (

        source,
        category,
        title,
        company,
        location,
        job_url,
        now,
        now

    ))


# ------------------------------
# INACTIVE JOB TRACKING
# ------------------------------

def mark_missing_jobs_inactive(

        cursor,
        source,
        category,
        scraped_urls

):

    if not scraped_urls:
        return


    placeholders = ",".join("?" * len(scraped_urls))


    query = f"""

        UPDATE jobs

        SET is_active = 0

        WHERE source = ?

        AND category = ?

        AND job_url NOT IN ({placeholders})

    """


    cursor.execute(

        query,

        [source, category, *scraped_urls]

    )


# ------------------------------
# SEARCH ENGINE
# ------------------------------
def search_jobs(
    cursor,
    keyword=None,
    category=None,
    company=None,
    source=None,
    location=None,
    active_only=True,
    sort_by="last_seen DESC",   # NEW: allow sorting control
    limit=50,
    offset=0
):

    params = []

    # --------------------------------
    # BASE QUERY
    # --------------------------------

    # REFINED: Always reference table name to avoid
    # "ambiguous column name" errors when JOIN is used
    if keyword:

        query = """
        SELECT jobs.*
        FROM jobs
        JOIN jobs_fts
        ON jobs.id = jobs_fts.rowid
        WHERE jobs_fts MATCH ?
        """

        params.append(keyword + "*")

    else:

        # REFINED: use jobs.* instead of *
        query = "SELECT jobs.* FROM jobs WHERE 1=1"


    # --------------------------------
    # FILTERS
    # --------------------------------

    if active_only:

        # REFINED: explicit table name
        query += " AND jobs.is_active = 1"


    if category:

        # REFINED: prevent ambiguous column
        query += " AND jobs.category = ?"

        params.append(category)


    if source:

        query += " AND jobs.source = ?"

        params.append(source)


    if company:

        query += " AND LOWER(jobs.company) LIKE ?"

        params.append(f"%{company.lower()}%")


    if location:

        query += " AND LOWER(jobs.location) LIKE ?"

        params.append(f"%{location.lower()}%")


    # --------------------------------
    # SORTING
    # --------------------------------

    # REFINED: dynamic sorting from UI
    # but with safety fallback
    allowed_sort_columns = [
        "last_seen DESC",
        "company ASC",
        "category ASC"
    ]

    if sort_by not in allowed_sort_columns:

        sort_by = "last_seen DESC"

    query += f" ORDER BY {sort_by}"


    # --------------------------------
    # PAGINATION
    # --------------------------------

    query += " LIMIT ? OFFSET ?"

    params.extend([limit, offset])


    # --------------------------------
    # EXECUTE QUERY
    # --------------------------------

    try:

        cursor.execute(query, params)

    except Exception as e:

        print("\nQUERY FAILED:")
        print(query)
        print(params)

        raise e


    rows = cursor.fetchall()

    return [dict(row) for row in rows]