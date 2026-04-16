import streamlit as st
import os
import time
import logging
import asyncio
import shutil
import zipfile
import io
from datetime import datetime, timedelta
from main_scrape import run_parallel_scrape
from parse_html_data import parse_twitter_html
from tweets_json_to_csv import run_twitter_conversion_pipeline

# --- Custom Logging Handler ---
class StreamlitLogHandler(logging.Handler):
    def __init__(self, log_placeholder):
        super().__init__()
        self.log_placeholder = log_placeholder
        self.log_buffer = []

    def emit(self, record):
        log_entry = self.format(record)
        self.log_buffer.append(log_entry)
        # Keep only the last 50 lines for UI performance
        if len(self.log_buffer) > 50:
            self.log_buffer.pop(0)
        # Update the placeholder in Streamlit
        self.log_placeholder.code("\n".join(self.log_buffer))

# --- Page Config ---
st.set_page_config(
    page_title="Twitter Scraper v2",
    page_icon="🤖",
    layout="wide"
)

# Define the Nitter filter keys and their UI labels
FILTER_OPTIONS = [
    ("nativeretweets", "Retweets"),
    ("media", "Media"),
    ("videos", "Videos"),
    ("news", "News"),
    ("native_video", "Native videos"),
    ("replies", "Replies"),
    ("links", "Links"),
    ("images", "Images"),
    ("quote", "Quotes"),
    ("spaces", "Spaces")
]

# --- Session State Initialization ---
def create_default_task(task_id, keyword=""):
    task = {
        "id": task_id,
        "job_type": "Search",
        "keyword": keyword,
        "since": datetime.now() - timedelta(days=7),
        "until": datetime.now(),
        "max_pages": 3,
        "min_faves": 0,
        "min_retweets": 0,
        "min_replies": 0
    }
    # Initialize all include/exclude toggles to False
    for key, label in FILTER_OPTIONS:
        task[f"include_{key}"] = False
        task[f"exclude_{key}"] = False
    return task

if 'tasks' not in st.session_state:
    st.session_state.tasks = [create_default_task(0, "Job 1")]
if 'task_counter' not in st.session_state:
    st.session_state.task_counter = 1

# --- Styling ---
st.markdown("""
<style>
    .stExpander { border: 1px solid #1DA1F2; border-radius: 8px; margin-bottom: 10px; }
    .stButton>button { border-radius: 20px; }
    .remove-btn>button { background-color: #ff4b4b; color: white; }
    .filter-header { color: #1DA1F2; font-weight: bold; margin-bottom: 5px; border-bottom: 1px solid #1DA1F2; }
</style>
""", unsafe_allow_html=True)

# --- Sidebar Controls ---
with st.sidebar:
    st.title("⚙️ Global Controls")
    st.info("Manage your scraping data and pipeline execution.")
    
    if st.button("🗑️ Manually Clear Raw Data", help="Deletes all folders in the data directory"):
        if os.path.exists("data"):
            shutil.rmtree("data")
            os.makedirs("data")
            st.success("Data directory cleared.")
        else:
            st.warning("Data directory does not exist.")

    auto_clear = st.checkbox("Auto-clear data before run", value=True, help="Automatically wipe previous scrape data when starting a new run to ensure fresh results.")

    st.divider()
    st.header("1. Define Tasks")
    if st.button("➕ Add New Keyword Job"):
        st.session_state.tasks.append(create_default_task(st.session_state.task_counter, f"Job {st.session_state.task_counter + 1}"))
        st.session_state.task_counter += 1

    st.divider()
    st.header("2. Output Format")
    output_formats = st.multiselect("Select Formats", ["JSON", "CSV"], default=["JSON", "CSV"])

    st.divider()
    execute = st.button("🚀 RUN PARALLEL SCRAPE", type="primary", use_container_width=True)

# --- Main UI ---
st.title("🤖 Twitter Async Scraper Dashboard")
st.markdown("Run multiple high-performance scraping jobs in parallel using Async HTTPX.")

# Display and Configure Tasks
to_remove = []
for i, task in enumerate(st.session_state.tasks):
    with st.expander(f"📌 Task {i+1}: {task['keyword'] if task['keyword'] else 'New Job'}", expanded=True):
        col_main, col_del = st.columns([9, 1])
        
        with col_main:
            # Job Type Selector
            task['job_type'] = st.radio("Job Type", ["Search", "Profile"], index=0 if task.get('job_type') == "Search" else 1, key=f"type_{task['id']}", horizontal=True)

            # Core Config
            c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
            label = "Search Keyword" if task['job_type'] == "Search" else "User Handle (e.g. @elonmusk)"
            task['keyword'] = c1.text_input(label, value=task['keyword'], key=f"kw_{task['id']}")
            
            if task['job_type'] == "Search":
                task['since'] = c2.date_input("Since", value=task['since'], key=f"since_{task['id']}")
                task['until'] = c3.date_input("Until", value=task['until'], key=f"until_{task['id']}")
                task['max_pages'] = c4.number_input("Pages", min_value=1, value=task['max_pages'], key=f"pages_{task['id']}")

                # Thresholds
                st.markdown("<div class='filter-header'>Thresholds</div>", unsafe_allow_html=True)
                f1, f2, f3 = st.columns(3)
                task['min_faves'] = f1.number_input("Minimum Likes", min_value=0, value=task['min_faves'], key=f"faves_{task['id']}")
                task['min_retweets'] = f2.number_input("Minimum Retweets", min_value=0, value=task['min_retweets'], key=f"rt_{task['id']}")
                task['min_replies'] = f3.number_input("Minimum Replies", min_value=0, value=task['min_replies'], key=f"rep_{task['id']}")
            else:
                # Profile Mode Simple Config
                task['max_pages'] = c2.number_input("Pages to Scrape", min_value=1, value=task['max_pages'], key=f"pages_{task['id']}")
                st.info("Profile scraping fetches the latest posts from the user's timeline. Threshold filters are not applicable.")

            # Toggles - Dual Layer
            # Filter (Include) Section
            st.markdown("<div class='filter-header'>Filter (Include)</div>", unsafe_allow_html=True)
            inc_cols = st.columns(5)
            for idx, (key, label) in enumerate(FILTER_OPTIONS):
                col_idx = idx % 5
                task[f"include_{key}"] = inc_cols[col_idx].checkbox(label, value=task[f"include_{key}"], key=f"inc_{key}_{task['id']}")

            # Exclude Section
            st.markdown("<div class='filter-header'>Exclude</div>", unsafe_allow_html=True)
            exc_cols = st.columns(5)
            for idx, (key, label) in enumerate(FILTER_OPTIONS):
                col_idx = idx % 5
                task[f"exclude_{key}"] = exc_cols[col_idx].checkbox(label, value=task[f"exclude_{key}"], key=f"exc_{key}_{task['id']}")
            
        with col_del:
            if st.button("🗑️", key=f"del_{task['id']}", help="Remove this task"):
                to_remove.append(i)

# Handle task removal
if to_remove:
    for index in sorted(to_remove, reverse=True):
        st.session_state.tasks.pop(index)
    st.rerun()

# --- Execution Logic ---
if execute:
    valid_tasks = [t for t in st.session_state.tasks if t['keyword'].strip()]
    if not valid_tasks:
        st.error("No valid tasks with keywords found.")
    else:
        # Prepare configs (converting dates to strings)
        search_jobs = []
        for t in valid_tasks:
            job = t.copy()
            job['since'] = job['since'].strftime("%Y-%m-%d")
            job['until'] = job['until'].strftime("%Y-%m-%d")
            search_jobs.append(job)

        # 1. Start Scraping
        st.subheader("📋 Pipeline Progress")
        
        # Log Box for Live Output
        st.markdown("<div class='filter-header'>Live Execution Logs</div>", unsafe_allow_html=True)
        log_placeholder = st.empty()
        log_placeholder.code("Waiting for logs...")

        # Setup Logging Handler
        logger = logging.getLogger()
        streamlit_handler = StreamlitLogHandler(log_placeholder)
        streamlit_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(streamlit_handler)

        with st.status("Executing Parallel Scrape...", expanded=True) as status:
            if auto_clear and os.path.exists("data"):
                st.write("🧹 Clearing stale raw data...")
                shutil.rmtree("data")
                os.makedirs("data")

            st.write("Initializing Async Client...")
            try:
                asyncio.run(run_parallel_scrape(search_jobs))
                
                st.write("Extraction complete. Starting Parser...")
                # 2. Run Recursive Parser
                jsonl_file = "extracted_tweets.jsonl"
                csv_file = "extracted_tweets.csv"
                
                tweet_count = parse_twitter_html("data", jsonl_file)
                st.write(f"Parsed {tweet_count} unique tweets.")

                # 3. Convert to CSV
                if "CSV" in output_formats and os.path.exists(jsonl_file):
                    st.write("Converting to CSV...")
                    run_twitter_conversion_pipeline(jsonl_file, csv_file)

                status.update(label="✅ Pipeline Completed!", state="complete", expanded=False)
            finally:
                # Always remove handler to avoid memory leaks and duplicate logs in future runs
                logger.removeHandler(streamlit_handler)

        st.success(f"Successfully processed {len(valid_tasks)} search jobs.")
        st.balloons()

        # Download Section
        st.divider()
        st.subheader("📦 Download Results")
        d1, d2, d3 = st.columns(3)
        
        if "JSON" in output_formats and os.path.exists(jsonl_file):
            with open(jsonl_file, "rb") as f:
                d1.download_button("📄 JSONL Result", f, file_name="tweets.jsonl", mime="application/json", use_container_width=True)
        
        if "CSV" in output_formats and os.path.exists(csv_file):
            with open(csv_file, "rb") as f:
                d2.download_button("📊 CSV Result", f, file_name="tweets.csv", mime="text/csv", use_container_width=True)

        # ZIP Download Logic
        if os.path.exists(jsonl_file) and os.path.exists(csv_file):
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(jsonl_file, arcname="tweets.jsonl")
                zf.write(csv_file, arcname="tweets.csv")
            
            d3.download_button(
                "🎁 Download All (ZIP)",
                zip_buffer.getvalue(),
                file_name="twitter_scrape_results.zip",
                mime="application/zip",
                use_container_width=True
            )

# Footer
st.divider()
st.caption("Powered by Async HTTPX & Streamlit")
