import streamlit as st
from database import search_jobs, connect_db
from database import create_tables

# Open connection safely
conn, cursor = connect_db()

# Create tables if not exist (prevents crash)
create_tables(cursor)
conn.commit()
conn.close()

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(
    page_title="Job Search Engine",
    layout="wide"
)

st.title("Job Search Engine")

# -----------------------------------
# SESSION STATE INITIALIZATION
# -----------------------------------

# REFINED: session state for pagination
if "page" not in st.session_state:
    st.session_state.page = 0

# REFINED: track if search was triggered
if "search_triggered" not in st.session_state:
    st.session_state.search_triggered = False

# NEW FEATURE: saved jobs storage
if "saved_jobs" not in st.session_state:
    st.session_state.saved_jobs = []

PAGE_SIZE = 10

# -----------------------------------
# LOAD FILTER VALUES FROM DATABASE
# -----------------------------------
# NEW FEATURE: dynamic filter options

def load_filter_options():
    conn, cursor = connect_db()

    cursor.execute("SELECT DISTINCT category FROM jobs ORDER BY category")
    categories = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT company FROM jobs ORDER BY company")
    companies = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT location FROM jobs ORDER BY location")
    locations = [row[0] for row in cursor.fetchall()]

    conn.close()

    return categories, companies, locations


categories, companies, locations = load_filter_options()

# -----------------------------------
# SIDEBAR FILTERS
# -----------------------------------

st.sidebar.header("Filters")

# REFINED: Selectboxes instead of text input
category = st.sidebar.selectbox(
    "Category",
    ["All"] + categories
)

company = st.sidebar.selectbox(
    "Company",
    ["All"] + companies
)

location = st.sidebar.selectbox(
    "Location",
    ["All"] + locations
)

active_only = st.sidebar.checkbox(
    "Active jobs only",
    value=True
)

# -----------------------------------
# SORTING FEATURE
# -----------------------------------
# NEW FEATURE: sorting dropdown

sort_option = st.sidebar.selectbox(
    "Sort by",
    [
        "Most Recent",
        "Company A–Z",
        "Category"
    ]
)

# Convert UI option into SQL clause
if sort_option == "Most Recent":
    sort_sql = "posted_date DESC"

elif sort_option == "Company A–Z":
    sort_sql = "company ASC"

else:
    sort_sql = "category ASC"

# -----------------------------------
# SEARCH INPUT
# -----------------------------------

keyword = st.text_input(
    "Keyword",
    placeholder="e.g. Python Developer"
)

search_button = st.button("Search Jobs")


# -----------------------------------
# SEARCH BUTTON LOGIC
# -----------------------------------

if search_button:

    # REFINED: prevent empty searches
    if not any([keyword, category != "All", company != "All", location != "All"]):

        st.warning(
            "Please enter at least one filter or keyword."
        )

        st.stop()

    st.session_state.page = 0
    st.session_state.search_triggered = True


# -----------------------------------
# SEARCH EXECUTION
# -----------------------------------

def run_search():

    conn, cursor = connect_db()

    offset = st.session_state.page * PAGE_SIZE

    # REFINED: convert "All" → None for query
    results = search_jobs(
        cursor,
        keyword=keyword or None,
        category=None if category == "All" else category,
        company=None if company == "All" else company,
        location=None if location == "All" else location,
        active_only=active_only,
        limit=PAGE_SIZE,
        offset=offset,
        sort_by=sort_sql
    )

    # NEW FEATURE: total results count query
    cursor.execute("SELECT COUNT(*) FROM jobs")
    total_results = cursor.fetchone()[0]

    conn.close()

    return results, total_results


# -----------------------------------
# DISPLAY RESULTS
# -----------------------------------

if st.session_state.search_triggered:

    with st.spinner("Searching jobs..."):

        results, total_results = run_search()

    st.divider()

    if results:

        # REFINED: professional pagination counter
        start = st.session_state.page * PAGE_SIZE + 1
        end = min((st.session_state.page + 1) * PAGE_SIZE, total_results)

        st.success(
            f"Showing {start}–{end} of {total_results} jobs"
        )

        for job in results:

            with st.expander(job["title"]):

                st.write("**Company:**", job["company"])
                st.write("**Location:**", job["location"])
                st.write("**Category:**", job["category"])
                st.write("**Source:**", job["source"])

                st.markdown(
                    f"[Open Job]({job['job_url']})"
                )

                # NEW FEATURE: save job button
                if st.button(
                    "Save Job",
                    key=f"save_{job['job_url']}"
                ):
                    st.session_state.saved_jobs.append(job)

                    st.success("Job saved!")

        # -----------------------------------
        # PAGINATION
        # -----------------------------------

        col1, col2, col3 = st.columns([1, 2, 1])

        with col1:

            if st.button(
                "Previous Page",
                key="prev_page"
            ):

                if st.session_state.page > 0:

                    st.session_state.page -= 1
                    st.rerun()

        with col3:

            if len(results) == PAGE_SIZE:

                if st.button("Next Page"):

                    st.session_state.page += 1
                    st.rerun()

    else:

        st.warning("No jobs found.")


# -----------------------------------
# SAVED JOBS SECTION
# -----------------------------------
# NEW FEATURE: bookmark list

if st.session_state.saved_jobs:

    st.divider()

    st.subheader(
        f"Saved Jobs ({len(st.session_state.saved_jobs)})"
    )

    for job in st.session_state.saved_jobs:

        with st.expander(job["title"]):

            st.write("**Company:**", job["company"])
            st.write("**Location:**", job["location"])

            st.markdown(
                f"[Open Job]({job['job_url']})"
            )