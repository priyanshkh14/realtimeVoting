import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import psycopg2

# Function to create a Kafka consumer (you may not need this for aggregated data if from DB)
def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

# Fetch total voters and candidates counts from DB
@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM voters")
    voters_count = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM candidates")
    candidates_count = cur.fetchone()[0]
    conn.close()
    return voters_count, candidates_count

# Fetch aggregated votes per candidate from DB
@st.cache_data
def fetch_aggregated_votes_per_candidate():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    query = """
        SELECT c.candidate_id, c.candidate_name, c.party_affiliation, c.photo_url, 
               COUNT(v.voter_id) AS total_votes
        FROM candidates c
        LEFT JOIN votes v ON c.candidate_id = v.candidate_id
        GROUP BY c.candidate_id, c.candidate_name, c.party_affiliation, c.photo_url
        ORDER BY total_votes DESC;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Plot bar chart with colors
def plot_colored_bar_chart(results):
    candidates = results['candidate_name']
    votes = results['total_votes']
    colors = plt.cm.viridis(np.linspace(0, 1, len(candidates)))
    fig, ax = plt.subplots()
    ax.bar(candidates, votes, color=colors)
    ax.set_xlabel('Candidate')
    ax.set_ylabel('Total Votes')
    ax.set_title('Vote Counts per Candidate')
    plt.xticks(rotation=90)
    plt.tight_layout()
    return fig

# Plot donut chart for vote distribution
def plot_donut_chart(data: pd.DataFrame, title='Vote Distribution'):
    labels = data['candidate_name'].tolist()
    sizes = data['total_votes'].tolist()
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, wedgeprops=dict(width=0.4))
    ax.axis('equal')
    plt.title(title)
    return fig

# Split dataframe into chunks (pagination helper)
@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    return [input_df.iloc[i:i + rows] for i in range(0, len(input_df), rows)]

# Paginate and display table with sorting
def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=True, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
        ascending = sort_direction == "⬆️"
        table_data = table_data.sort_values(by=sort_field, ascending=ascending, ignore_index=True)
    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = max(int(len(table_data) / batch_size), 1)
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

# Main data update function for Streamlit dashboard
def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch voting stats
    voters_count, candidates_count = fetch_voting_stats()

    st.markdown("---")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    # Fetch aggregated vote counts per candidate from DB
    results = fetch_aggregated_votes_per_candidate()

    if results.empty or 'candidate_id' not in results.columns:
        st.warning("No vote data available yet.")
        return

    # Identify leading candidate
    leading_candidate = results.iloc[results['total_votes'].idxmax()]

    st.markdown("---")
    st.header('Leading Candidate')
    col1, col2 = st.columns([1, 2])
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader(f"Total Votes: {leading_candidate['total_votes']}")

    st.markdown("---")
    st.header('Statistics')
    display_results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']].reset_index(drop=True)

    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(display_results)
        st.pyplot(bar_fig)
    with col2:
        donut_fig = plot_donut_chart(display_results)
        st.pyplot(donut_fig)

    st.table(display_results)

    st.session_state['last_update'] = time.time()

# Sidebar controls for refresh and manual update
def sidebar():
    if 'last_update' not in st.session_state:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    if st.sidebar.button('Refresh Data'):
        update_data()

# Streamlit app entry point
st.title('Real-time Election Dashboard')

sidebar()
update_data()
