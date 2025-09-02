import streamlit as st
import pandas as pd
import requests
import time
import io
import csv
import re
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Strategy Bot Query Tool",
    page_icon="🤖",
    layout="wide"
)

# Check if we need to reset state based on query parameters
if "reset" in st.query_params:
    # Clear all session state if reset parameter is present
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    # Remove the reset parameter
    st.query_params.clear()

# Initialize session state variables if they don't exist
if 'has_results' not in st.session_state:
    st.session_state.has_results = False
if 'results_df' not in st.session_state:
    st.session_state.results_df = None
if 'results_filename' not in st.session_state:
    st.session_state.results_filename = None
if 'file_uploader_key' not in st.session_state:
    st.session_state.file_uploader_key = "initial"

# Function to reset the session state via URL parameter
def reset_session():
    # Set URL parameter to trigger reset on next load
    st.query_params["reset"] = "true"
    # Rerun the app to apply the reset
    st.experimental_rerun()

# App title and description
st.title("Strategy Bot Query Tool")

# Instructions
st.markdown("""
Use this site to send multiple queries to a bot and test for accuracy. Add your bot information and credentials, 
then attach a CSV file with all the questions you want to ask and then click "Run Queries". 
We'll let you know how long the process will take and then when you come back you'll be able to 
download a file with all the questions, answers, interpretations, SQL queries, and response times to judge the performance of your bot.
""")

# Function to analyze SQL complexity and estimate latency
def analyze_sql_complexity(sql_query):
    """
    Analyze SQL complexity and return estimated latency in seconds.
    
    Complexity levels:
    - No SQL: 0.5 seconds
    - Simple SQL: 3 seconds 
    - Complex SQL: 5 seconds
    - Very Complex SQL: 8 seconds
    """
    if not sql_query or len(sql_query.strip()) < 10:
        return 0.5, "No SQL"  # No meaningful SQL
    
    # Convert to lowercase for easier pattern matching
    sql_lower = sql_query.lower()
    
    # Very complex patterns (multiple joins, complex functions, subqueries, window functions)
    very_complex_patterns = [
        r'with\s+.*\s+as',        # CTE (Common Table Expressions)
        r'over\s*\(',              # Window functions
        r'(select.*from.*?)\s+select', # Subqueries
        r'join.*join.*join',       # Multiple joins (3+)
        r'case\s+when.*case\s+when', # Nested CASE statements
        r'union|intersect|except'  # Set operations
    ]
    
    # Complex patterns (joins, aggregations, group by)
    complex_patterns = [
        r'join',                   # Any kind of join
        r'group\s+by',             # Grouping
        r'having',                 # Having clause
        r'order\s+by.*order\s+by', # Multiple order by clauses
        r'distinct',               # Distinct operations
        r'sum\(|avg\(|count\(|max\(|min\(' # Aggregations
    ]
    
    # Simple patterns (basic where clauses, single table, order by)
    simple_patterns = [
        r'where',                  # Where clause
        r'order\s+by',             # Order by
        r'limit',                  # Limit clause
        r'select.*from'            # Basic select
    ]
    
    # Check for very complex patterns
    for pattern in very_complex_patterns:
        if re.search(pattern, sql_lower):
            return 8.0, "Very Complex SQL"
    
    # Check for complex patterns
    for pattern in complex_patterns:
        if re.search(pattern, sql_lower):
            return 5.0, "Complex SQL"
    
    # If we've gotten here, it's either simple or has unusual patterns
    # Let's check for simple patterns
    for pattern in simple_patterns:
        if re.search(pattern, sql_lower):
            return 3.0, "Simple SQL"
    
    # Default to simple if we can't determine (but it has some SQL)
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
        Poll for answer with basic first response timing
        Returns: (response_data, time_to_first_response, total_response_time)
        """
        start_time = time.time()
        url = f"{self.base_url}/api/questions/{question_id}"
        first_response_time = None
        
        # Try to get initial response quickly (shorter intervals)
        for _ in range(30):  # Try for 3 seconds (30 * 0.1)
            if time.time() - start_time > timeout:
                raise TimeoutError("Polling timed out")
                
            response = self.session.get(url)
            
            # If we get any response with status 200, mark first response time
            if response.status_code == 200:
                first_response_time = time.time() - start_time
                break
                
            time.sleep(0.1)
        
        # If we still don't have first response time, set it equal to whenever we get the final answer
        if first_response_time is None:
            first_response_time = time.time() - start_time
            
        # Continue polling until completion
        while (time.time() - start_time) < timeout:
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()
                # Check if we have a complete answer
                if "answers" in data and len(data["answers"]) > 0:
                    # Consider it complete if it has answer text
                    if "text" in data["answers"][0] and data["answers"][0]["text"]:
                        return data, first_response_time, time.time() - start_time
            
            elif response.status_code != 202:
                response.raise_for_status()

            time.sleep(interval)

        raise TimeoutError("Polling timed out after 5 minutes")
        
    def extract_data_from_response(self, response_data):
        """
        Extract interpretation, SQL, and insights from the response.
        Returns: (interpretation, sql, insights)
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
            
            # Extract insights if available
            if "insights" in answer:
                # Handle insights based on data structure (could be string or object)
                if isinstance(answer["insights"], str):
                    insights = answer["insights"]
                elif isinstance(answer["insights"], list) and len(answer["insights"]) > 0:
                    # If it's a list of objects, try to extract text from each
                    insights_texts = []
                    for insight in answer["insights"]:
                        if isinstance(insight, str):
                            insights_texts.append(insight)
                        elif isinstance(insight, dict) and "text" in insight:
                            insights_texts.append(insight["text"])
                    insights = "\n".join(insights_texts)
        
        return interpretation, sql, insights

# Parse the CSV file to extract questions - fixed for binary file handling
def parse_questions_from_csv(file):
    questions = []
    
    # Reset file pointer to beginning
    file.seek(0)
    
    try:
        # Convert bytes to string for CSV reader
        text_content = io.TextIOWrapper(file, encoding='utf-8')
        csv_reader = csv.reader(text_content)
        rows = list(csv_reader)
        
        # If we have at least one row
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
                # No header found, assume first column has questions including first row
                for row in rows:
                    if row and row[0].strip():
                        questions.append(row[0].strip())
                        
    except Exception as e:
        st.error(f"Error reading CSV file: {str(e)}")
        # If CSV reading fails, try pandas as fallback
        try:
            file.seek(0)
            csv_data = pd.read_csv(file)  # Pandas handles binary files automatically
            # If we have at least one column, take the first column
            if len(csv_data.columns) > 0:
                questions = csv_data.iloc[:, 0].dropna().tolist()
        except Exception as inner_e:
            st.error(f"Fallback CSV reading also failed: {str(inner_e)}")
    
    return questions

# Run queries function
def run_queries(questions_list):
    # Create results DataFrame with additional assessment columns
    results_df = pd.DataFrame(columns=[
        "Question", 
        "Answer", 
        "Insights",  # Moved to be after Answer
        "Interpretation", 
        "SQL", 
        "API Response Time (seconds)",  # Renamed from "Time to First Response"
        "Total Response Time (seconds)",
        "Estimated Time to First Response (seconds)",  # Renamed from "Estimated LLM Processing Time"
        "Question Difficulty (1-5)",
        "Pass/Fail",
        "Answer Accuracy (1-5)"
    ])
    
    # Initialize client
    client = ChatbotClient(base_url, bot_id, project_id)
    
    # Login
    with st.spinner("Logging in..."):
        try:
            login_success = client.login(username, password)
            if not login_success:
                st.error("Login failed. Please check your credentials.")
                return None
        except Exception as e:
            st.error(f"Login error: {str(e)}")
            return None
    
    st.success("Login successful!")
    
    # Create progress elements
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Calculate estimated time (20 seconds delay + ~15 seconds response time)
    total_questions = len(questions_list)
    delay_between_questions = 20
    estimated_time = total_questions * (delay_between_questions + 15)
    st.info(f"Estimated time: {estimated_time//60} minutes {estimated_time%60} seconds")
    
    # Process each question
    for i, question in enumerate(questions_list):
        # Update progress
        progress = int((i / total_questions) * 100)
        progress_bar.progress(progress)
        status_text.text(f"Processing question {i+1}/{total_questions}: {question}")
        
        try:
            # Submit question
            question_id = client.submit_question(question)
            
            # Poll for answer with timing information
            result, first_response_time, total_response_time = client.poll_answer(question_id)
            
            # Extract data
            answer_text = result["answers"][0]["text"] if "answers" in result and len(result["answers"]) > 0 else "No answer provided"
            interpretation, sql, insights = client.extract_data_from_response(result)
            
            # Analyze SQL complexity and get estimated latency
            latency, complexity = analyze_sql_complexity(sql)
            
            # Calculate estimated time to first response (API response time + SQL complexity latency)
            estimated_first_response = first_response_time + latency
            
            # Add to DataFrame with empty assessment columns
            results_df.loc[len(results_df)] = [
                question,
                answer_text,
                insights,       # Moved to be after Answer
                interpretation,
                sql,
                round(first_response_time, 2),
                round(total_response_time, 2),
                round(estimated_first_response, 2),
                "",  # Question Difficulty - left empty for user to fill
                "",  # Pass/Fail - left empty for user to fill
                ""   # Answer Accuracy - left empty for user to fill
            ]
            
            # Show intermediate result - UPDATED LABELS
            st.success(f"""✓ Got answer for question {i+1}:
            - API Response Time: {first_response_time:.2f}s
            - Total Response Time: {total_response_time:.2f}s
            - SQL Complexity: {complexity} (+{latency:.1f}s)
            - Estimated Time to First Response: {estimated_first_response:.2f}s""")
            
        except Exception as e:
            st.error(f"Error processing question {i+1}: {str(e)}")
            
            # Add error to DataFrame with empty assessment columns
            results_df.loc[len(results_df)] = [
                question,
                f"ERROR: {str(e)}",
                "",
                "",
                "",
                0,
                0,
                0,
                "",  # Question Difficulty
                "Fail",  # Auto-fill as fail since there was an error
                ""   # Answer Accuracy
            ]
        
        # Delay before next question
        if i < total_questions - 1:
            status_text.text(f"Waiting {delay_between_questions} seconds before next question...")
            time.sleep(delay_between_questions)
    
    # Update progress to 100%
    progress_bar.progress(100)
    status_text.text("All questions processed!")
    
    return results_df

# Function to create a downloadable CSV
def create_download_csv(df):
    # Create a CSV string from the DataFrame
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()
    
    # Return the CSV data
    return csv_string

# Add custom CSS styling for your interface
st.markdown("""
<style>
    .stButton > button {
        background-color: #4CAF50;
        color: white;
        font-size: 18px;
        padding: 10px 24px;
        border-radius: 8px;
    }
    .stButton > button:hover {
        background-color: #45a049;
    }
    .download-btn {
        background-color: #008CBA;
    }
    .stProgress > div > div {
        background-color: #4CAF50;
    }
    .right-align {
        text-align: right;
        width: 100%;
    }
    .warning-box {
        background-color: #fff3cd;
        padding: 10px 15px;
        border-left: 5px solid #ffc107;
        margin: 10px 0;
    }
    .new-test-btn button {
        background-color: #dc3545 !important;
    }
    .new-test-btn button:hover {
        background-color: #c82333 !important;
    }
</style>
""", unsafe_allow_html=True)

# Main app logic - check if we have existing results
if st.session_state.has_results and st.session_state.results_df is not None:
    # Display the existing results
    st.header("Test Results")
    
    # Display the dataframe
    display_df = st.session_state.results_df.drop(columns=["Question Difficulty (1-5)", "Pass/Fail", "Answer Accuracy (1-5)"])
    st.dataframe(display_df, use_container_width=True)
    
    # Create download button
    csv_data = create_download_csv(st.session_state.results_df)
    st.download_button(
        label="📥 Download CSV Results (includes assessment columns)",
        data=csv_data,
        file_name=st.session_state.results_filename,
        mime="text/csv",
        key="download_btn_results"
    )
    
    # Add note about assessment columns
    st.info("The downloaded CSV includes additional columns for manual assessment: 'Question Difficulty (1-5)', 'Pass/Fail', and 'Answer Accuracy (1-5)'.")
    
    # Add "Run New Test" button and warning below the results
    st.markdown("---")
    col1, col2 = st.columns([1, 3])
    with col1:
        st.markdown('<div class="new-test-btn">', unsafe_allow_html=True)
        if st.button("🔄 Run New Test", key="new_test_btn"):
            reset_session()  # This will add a URL parameter and trigger a page reload
        st.markdown('</div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<div class="warning-box">This will delete prior test results. Be sure you have downloaded all test results before starting a new test.</div>', unsafe_allow_html=True)

else:
    # Show the form to create a new test
    # Create columns for inputs
    input_col1, input_col2 = st.columns(2)

    with input_col1:
        # API Connection Settings
        st.subheader("Connection Settings")
        base_url = st.text_input("Base URL", value="https://autotrial.microstrategy.com/MicroStrategyLibrary")
        project_id = st.text_input("Project ID", value="205BABE083484404399FBBA37BAA874A")
        bot_id = st.text_input("Bot ID", value="1DC776FB20744B85AFEE148D7C11C842")

    with input_col2:
        # Authentication
        st.subheader("Authentication")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        # File upload - use the key from session state to force refresh
        st.subheader("Questions File")
        uploaded_file = st.file_uploader("Upload CSV file with questions", 
                                      type="csv", 
                                      key=st.session_state.file_uploader_key)

    # Process the uploaded file if we have one
    if uploaded_file is not None:
        # Parse questions from the uploaded CSV
        questions_list = parse_questions_from_csv(uploaded_file)
        
        if questions_list:
            st.write(f"Found {len(questions_list)} questions in the CSV file")
            
            # Show first few questions
            if len(questions_list) > 0:
                st.subheader("Sample Questions:")
                for i, q in enumerate(questions_list[:5]):
                    st.write(f"{i+1}. {q}")
                if len(questions_list) > 5:
                    st.write("...")
            
            # Create a container to properly align the button to the right
            btn_container = st.container()
            with btn_container:
                # Use HTML/CSS for right alignment
                st.markdown('<div class="right-align">', unsafe_allow_html=True)
                run_button_pressed = st.button("▶️ Run Queries", key="run_btn")
                st.markdown('</div>', unsafe_allow_html=True)
            
            if run_button_pressed:
                if not username or not password:
                    st.error("Please enter your username and password")
                else:
                    # Run queries and get results
                    results_df = run_queries(questions_list)
                    
                    if results_df is not None:
                        # Store results in session state
                        st.session_state.has_results = True
                        st.session_state.results_df = results_df
                        st.session_state.results_filename = f"bot_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                        
                        # Display results (hide assessment columns in the display)
                        st.subheader("Results")
                        display_df = results_df.drop(columns=["Question Difficulty (1-5)", "Pass/Fail", "Answer Accuracy (1-5)"])
                        st.dataframe(display_df, use_container_width=True)
                        
                        # Create download button for CSV with custom styling
                        st.markdown('<div class="download-section">', unsafe_allow_html=True)
                        csv_data = create_download_csv(results_df)
                        
                        st.download_button(
                            label="📥 Download CSV Results (includes assessment columns)",
                            data=csv_data,
                            file_name=st.session_state.results_filename,
                            mime="text/csv",
                            key="download_btn_new"
                        )
                        st.markdown('</div>', unsafe_allow_html=True)
                        
                        # Add note about assessment columns
                        st.info("The downloaded CSV includes additional columns for manual assessment: 'Question Difficulty (1-5)', 'Pass/Fail', and 'Answer Accuracy (1-5)'.")
                        
                        # Immediately show the Run New Test button and warning
                        st.markdown("---")
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.markdown('<div class="new-test-btn">', unsafe_allow_html=True)
                            if st.button("🔄 Run New Test", key="new_test_after_run"):
                                reset_session()  # This will add a URL parameter and trigger a page reload
                            st.markdown('</div>', unsafe_allow_html=True)
                        with col2:
                            st.markdown('<div class="warning-box">This will delete prior test results. Be sure you have downloaded all test results before starting a new test.</div>', unsafe_allow_html=True)
        else:
            st.error("No questions found in the CSV file. Please make sure the file contains questions.")
    else:
        st.info("Please upload a CSV file with questions to continue.")
