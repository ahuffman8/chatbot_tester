import streamlit as st
import pandas as pd
import requests
import time
import io
import csv
import re
from datetime import datetime
import concurrent.futures
from threading import Lock

# Page configuration
st.set_page_config(
    page_title="Strategy Bot Query Tool",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state variables
if 'results_df' not in st.session_state:
    st.session_state.results_df = None
if 'processed_questions' not in st.session_state:
    st.session_state.processed_questions = set()
if 'processing_started' not in st.session_state:
    st.session_state.processing_started = False
if 'questions_list' not in st.session_state:
    st.session_state.questions_list = []
if 'results_lock' not in st.session_state:
    st.session_state.results_lock = Lock()
if 'progress' not in st.session_state:
    st.session_state.progress = 0
if 'status_messages' not in st.session_state:
    st.session_state.status_messages = []
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False

# App title and description
st.title("Strategy Bot Query Tool")

# Instructions
st.markdown("""
Use this site to send multiple queries to a bot and test for accuracy. Add your bot information and credentials,
then attach a CSV file with all the questions you want to ask and then click "Run Queries".
We'll process your questions in parallel for maximum speed and efficiency.
""")

# Create columns for inputs
input_col1, input_col2 = st.columns(2)

with input_col1:
    # API Connection Settings
    st.subheader("Connection Settings")
    base_url = st.text_input("Base URL", value="https://autotrial.microstrategy.com/MicroStrategyLibrary")
    project_id = st.text_input("Project ID", value="205BABE083484404399FBBA37BAA874A")
    bot_id = st.text_input("Bot ID", value="1DC776FB20744B85AFEE148D7C11C842")
    
    # Parallel processing settings
    max_concurrent = st.slider("Maximum Concurrent Requests", min_value=2, max_value=20, value=5, 
                             help="Higher values process more questions simultaneously but may hit API rate limits")

with input_col2:
    # Authentication
    st.subheader("Authentication")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

# File upload
st.subheader("Questions File")
uploaded_file = st.file_uploader("Upload CSV file with questions", type="csv")

# Function to analyze SQL complexity and estimate latency
def analyze_sql_complexity(sql_query):
    """
    Analyze SQL complexity and return estimated latency in seconds.
    """
    if not sql_query or len(sql_query.strip()) < 10:
        return 0.5, "No SQL"  # No meaningful SQL

    # Convert to lowercase for easier pattern matching
    sql_lower = sql_query.lower()

    # Very complex patterns
    very_complex_patterns = [
        r'with\s+.*\s+as', r'over\s*\(', r'(select.*from.*?)\s+select',
        r'join.*join.*join', r'case\s+when.*case\s+when', r'union|intersect|except'
    ]

    # Complex patterns
    complex_patterns = [
        r'join', r'group\s+by', r'having', r'order\s+by.*order\s+by',
        r'distinct', r'sum\(|avg\(|count\(|max\(|min\('
    ]

    # Simple patterns
    simple_patterns = [r'where', r'order\s+by', r'limit', r'select.*from']

    # Check patterns from most to least complex
    for pattern in very_complex_patterns:
        if re.search(pattern, sql_lower):
            return 8.0, "Very Complex SQL"

    for pattern in complex_patterns:
        if re.search(pattern, sql_lower):
            return 5.0, "Complex SQL"

    for pattern in simple_patterns:
        if re.search(pattern, sql_lower):
            return 3.0, "Simple SQL"

    return 3.0, "Simple SQL"

# Chatbot client class
class ChatbotClient:
    def __init__(self, base_url, bot_id, project_id):
        self.base_url = base_url
        self.bot_id = bot_id
        self.project_id = project_id
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def login(self, username, password):
        """Authenticate and store token in session headers"""
        url = f"{self.base_url}/api/auth/login"
        payload = {
            "username": username,
            "password": password
        }
        
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        
        # Extract token from headers
        auth_token = response.headers.get("X-MSTR-AuthToken")
        if auth_token:
            self.session.headers.update({"X-MSTR-AuthToken": auth_token})
            return True
        return False

    def submit_question(self, question_text):
        """Submit new question and return question ID"""
        url = f"{self.base_url}/api/questions"
        headers = {
            "Prefer": "respond-async",
            "X-MSTR-ProjectID": self.project_id
        }
        payload = {
            "text": question_text,
            "textOnly": True,
            "bots": [{
                "id": self.bot_id,
                "projectId": self.project_id
            }],
            "history": []
        }

        response = self.session.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()["id"]

    def poll_answer(self, question_id, timeout=300, interval=1):
        """
        Poll for answer with response timing
        """
        start_time = time.time()
        url = f"{self.base_url}/api/questions/{question_id}"
        first_response_time = None
        
        # Try to get initial response quickly
        for _ in range(30):
            if time.time() - start_time > timeout:
                raise TimeoutError("Polling timed out")
                
            response = self.session.get(url)
            
            if response.status_code == 200:
                first_response_time = time.time() - start_time
                break
                
            time.sleep(0.1)
        
        if first_response_time is None:
            first_response_time = time.time() - start_time
            
        # Continue polling until completion
        while (time.time() - start_time) < timeout:
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()
                if "answers" in data and len(data["answers"]) > 0:
                    if "text" in data["answers"][0] and data["answers"][0]["text"]:
                        return data, first_response_time, time.time() - start_time
            
            elif response.status_code != 202:
                response.raise_for_status()

            time.sleep(interval)

        raise TimeoutError("Polling timed out")
        
    def extract_data_from_response(self, response_data):
        """
        Extract interpretation, SQL, and insights from the response.
        """
        interpretation = ""
        sql = ""
        insights = ""
        
        if "answers" in response_data and len(response_data["answers"]) > 0:
            answer = response_data["answers"][0]
            
            # Extract SQL queries
            if "sqlQueries" in answer and len(answer["sqlQueries"]) > 0:
                sql = answer["sqlQueries"][0]
            
            # Extract interpretation from queries
            if "queries" in answer and len(answer["queries"]) > 0:
                query = answer["queries"][0]
                if "explanation" in query:
                    interpretation = query["explanation"]
            
            # Extract insights
            if "insights" in answer:
                if isinstance(answer["insights"], str):
                    insights = answer["insights"]
                elif isinstance(answer["insights"], list) and len(answer["insights"]) > 0:
                    insights_texts = []
                    for insight in answer["insights"]:
                        if isinstance(insight, str):
                            insights_texts.append(insight)
                        elif isinstance(insight, dict) and "text" in insight:
                            insights_texts.append(insight["text"])
                    insights = "\n".join(insights_texts)
        
        return interpretation, sql, insights

# Parse the CSV file to extract questions
def parse_questions_from_csv(file):
    questions = []

    # Reset file pointer to beginning
    file.seek(0)

    try:
        # Convert bytes to string for CSV reader
        text_content = io.TextIOWrapper(file, encoding='utf-8')
        csv_reader = csv.reader(text_content)
        rows = list(csv_reader)
        
        if rows:
            # Check if the first row looks like a header
            first_row = rows[0]
            possible_headers = ["question", "questions", "query", "queries"]
            header_index = -1
            
            for i, cell in enumerate(first_row):
                if cell.lower() in possible_headers:
                    header_index = i
                    break
                    
            # If we found a header, use that column from row 1 onwards
            if header_index >= 0:
                for row in rows[1:]:
                    if len(row) > header_index and row[header_index].strip():
                        questions.append(row[header_index].strip())
            else:
                # No header found, assume first column has questions
                for row in rows:
                    if row and row[0].strip():
                        questions.append(row[0].strip())
                        
    except Exception as e:
        st.error(f"Error reading CSV file: {str(e)}")
        # If CSV reading fails, try pandas as fallback
        try:
            file.seek(0)
            csv_data = pd.read_csv(file)
            if len(csv_data.columns) > 0:
                questions = csv_data.iloc[:, 0].dropna().tolist()
        except Exception as inner_e:
            st.error(f"Fallback CSV reading also failed: {str(inner_e)}")

    return questions

# Function to process a single question in parallel
def process_single_question(args):
    question, index, total_count, base_url, bot_id, project_id, username, password = args
    
    try:
        # Create a new client instance for this question
        client = ChatbotClient(base_url, bot_id, project_id)
        
        # Login
        login_success = client.login(username, password)
        if not login_success:
            return {
                "question": question,
                "answer": "ERROR: Login failed",
                "insights": "",
                "interpretation": "",
                "sql": "",
                "api_response_time": 0,
                "total_response_time": 0,
                "estimated_time": 0,
                "index": index,
                "status": "failed"
            }
        
        # Submit question
        question_id = client.submit_question(question)
        
        # Poll for answer with timing information
        result, first_response_time, total_response_time = client.poll_answer(question_id)
        
        # Extract data
        answer_text = result["answers"][0]["text"] if "answers" in result and len(result["answers"]) > 0 else "No answer provided"
        interpretation, sql, insights = client.extract_data_from_response(result)
        
        # Analyze SQL complexity and get estimated latency
        latency, complexity = analyze_sql_complexity(sql)
        
        # Calculate estimated time to first response
        estimated_first_response = first_response_time + latency
        
        return {
            "question": question,
            "answer": answer_text,
            "insights": insights,
            "interpretation": interpretation,
            "sql": sql,
            "api_response_time": round(first_response_time, 2),
            "total_response_time": round(total_response_time, 2),
            "estimated_time": round(estimated_first_response, 2),
            "complexity": complexity,
            "index": index,
            "status": "success"
        }
    except Exception as e:
        return {
            "question": question,
            "answer": f"ERROR: {str(e)}",
            "insights": "",
            "interpretation": "",
            "sql": "",
            "api_response_time": 0,
            "total_response_time": 0,
            "estimated_time": 0,
            "index": index,
            "status": "failed"
        }

# Function to update progress and results
def update_results(future):
    try:
        result = future.result()
        with st.session_state.results_lock:
            # Get index from result
            index = result["index"]
            
            # Update processed questions set
            st.session_state.processed_questions.add(index)
            
            # Calculate new progress
            st.session_state.progress = len(st.session_state.processed_questions) / len(st.session_state.questions_list) * 100
            
            # Initialize results dataframe if needed
            if st.session_state.results_df is None:
                st.session_state.results_df = pd.DataFrame(columns=[
                    "Question", "Answer", "Insights", "Interpretation", "SQL", 
                    "API Response Time (seconds)", "Total Response Time (seconds)",
                    "Estimated Time to First Response (seconds)",
                    "Question Difficulty (1-5)", "Pass/Fail", "Answer Accuracy (1-5)"
                ])
            
            # Add result to dataframe
            st.session_state.results_df.loc[len(st.session_state.results_df)] = [
                result["question"],
                result["answer"],
                result["insights"],
                result["interpretation"],
                result["sql"],
                result["api_response_time"],
                result["total_response_time"],
                result["estimated_time"],
                "",  # Question Difficulty
                "Fail" if result["status"] == "failed" else "",  # Pass/Fail
                ""   # Answer Accuracy
            ]
            
            # Create status message
            if result["status"] == "success":
                complexity_info = f" | SQL: {result.get('complexity', 'N/A')}"
                message = f"✅ Q{index+1}: Processed in {result['total_response_time']}s{complexity_info}"
            else:
                message = f"❌ Q{index+1}: Failed - {result['answer']}"
                
            # Add to status messages (keep most recent 10)
            st.session_state.status_messages.append(message)
            if len(st.session_state.status_messages) > 10:
                st.session_state.status_messages.pop(0)
                
    except Exception as e:
        # Handle unexpected errors
        with st.session_state.results_lock:
            st.session_state.status_messages.append(f"⚠️ Error in callback: {str(e)}")

# Function to run queries in parallel
def run_queries_parallel(questions_list, max_workers):
    # Mark as not complete at the start
    st.session_state.processing_complete = False
    
    # Create a ThreadPoolExecutor for parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create futures for each question
        futures = []
        
        # Only process questions that haven't been processed yet
        unprocessed = [i for i in range(len(questions_list)) if i not in st.session_state.processed_questions]
        
        # If no unprocessed questions, mark as complete
        if not unprocessed:
            st.session_state.processing_complete = True
            return st.session_state.results_df
            
        # Submit all questions to the executor
        for i in unprocessed:
            question = questions_list[i]
            args = (question, i, len(questions_list), base_url, bot_id, project_id, username, password)
            future = executor.submit(process_single_question, args)
            future.add_done_callback(update_results)
            futures.append(future)
        
        # Wait for all futures to complete
        concurrent.futures.wait(futures)
    
    # Mark processing as complete
    st.session_state.processing_complete = True
    
    return st.session_state.results_df

# Function to create a downloadable CSV
def create_download_csv(df):
    # Create a CSV string from the DataFrame
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()
    return csv_string

# Add custom CSS styling for your interface
st.markdown("""
<style>
.stButton > button { background-color: #4CAF50; color: white; font-size: 18px; padding: 10px 24px; border-radius: 8px; }
.stButton > button:hover { background-color: #45a049; }
.download-btn { background-color: #008CBA; }
.stProgress > div > div { background-color: #4CAF50;
