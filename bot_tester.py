import streamlit as st
import pandas as pd
import requests
import time
import io
import csv
import re
import json
from datetime import datetime
import traceback

# Page configuration
st.set_page_config(
    page_title="Strategy Bot Query Tool",
    page_icon="🤖",
    layout="wide"
)

# App title and description
st.title("Strategy Bot Query Tool")

# Instructions
st.markdown("""
Use this site to send multiple queries to a bot and test for accuracy. Add your bot information and credentials,
then attach a CSV file with all the questions you want to ask and then click "Run Queries".
We'll let you know how long the process will take and then when you come back you'll be able to
download a file with all the questions, answers, interpretations, SQL queries, and response times to judge the performance of your bot.
""")

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

# File upload
st.subheader("Questions File")
uploaded_file = st.file_uploader("Upload CSV file with questions", type="csv")

# Initialize session state for checkpointing
if 'results_df' not in st.session_state:
    st.session_state.results_df = None
if 'questions_processed' not in st.session_state:
    st.session_state.questions_processed = 0
if 'total_questions' not in st.session_state:
    st.session_state.total_questions = 0
if 'questions_list' not in st.session_state:
    st.session_state.questions_list = []
if 'processing_active' not in st.session_state:
    st.session_state.processing_active = False

# Function to analyze SQL complexity and estimate latency
def analyze_sql_complexity(sql_query):
    """
    Analyze SQL complexity and return estimated latency in seconds.
    
    Complexity levels:
    
    No SQL: 0.5 seconds
    Simple SQL: 3 seconds
    Complex SQL: 5 seconds
    Very Complex SQL: 8 seconds
    """
    if not sql_query or len(sql_query.strip()) < 10:
        return 0.5, "No SQL"  # No meaningful SQL
        
    # Convert to lowercase for easier pattern matching
    sql_lower = sql_query.lower()
    
    # Very complex patterns (multiple joins, complex functions, subqueries, window functions)
    very_complex_patterns = [
        r'with\s+.\s+as',        # CTE (Common Table Expressions)
        r'over\s\(',              # Window functions
        r'(select.+from.+)\s+select', # Subqueries
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
        self.auth_token = None
        self.last_activity = time.time()
        
    def login(self, username, password):
        """Authenticate and store token in session headers"""
        url = f"{self.base_url}/api/auth/login"
        payload = {
            "username": username,
            "password": password
        }
        
        try:
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            # Extract token from headers
            self.auth_token = response.headers.get("X-MSTR-AuthToken")
            if self.auth_token:
                self.session.headers.update({"X-MSTR-AuthToken": self.auth_token})
                self.last_activity = time.time()
                return True
            return False
        except Exception as e:
            st.error(f"Login error: {str(e)}")
            return False
    
    def refresh_session(self, username, password):
        """Refresh the session by logging in again"""
        st.warning("Refreshing session...")
        time.sleep(2)  # Brief delay before retry
        success = self.login(username, password)
        if success:
            st.success("Session refreshed successfully!")
        else:
            st.error("Failed to refresh session")
        return success
    
    def check_session_age(self, username, password, max_age=900):
        """Check if session needs refreshing (15 minutes by default)"""
        current_time = time.time()
        if current_time - self.last_activity > max_age:
            return self.refresh_session(username, password)
        return True
        
    def submit_question(self, question_text, username, password, max_retries=3):
        """Submit new question and return question ID with retries"""
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
        
        retries = 0
        while retries <= max_retries:
            try:
                # Always check session before submitting
                if retries > 0 or not self.check_session_age(username, password):
                    self.refresh_session(username, password)
                
                response = self.session.post(url, headers=headers, json=payload, timeout=30)
                response.raise_for_status()
                self.last_activity = time.time()
                return response.json()["id"]
            except requests.exceptions.HTTPError as e:
                retries += 1
                st.warning(f"HTTP error when submitting question (attempt {retries}/{max_retries+1}): {str(e)}")
                if retries <= max_retries:
                    time.sleep(2 * retries)  # Exponential backoff
                    continue
                else:
                    raise
            except Exception as e:
                retries += 1
                st.warning(f"Error submitting question (attempt {retries}/{max_retries+1}): {str(e)}")
                if retries <= max_retries:
                    time.sleep(2 * retries)  # Exponential backoff
                    continue
                else:
                    raise Exception(f"Error submitting question after {max_retries+1} attempts: {str(e)}")
        
        raise Exception("Maximum retries exceeded when submitting question")
        
    def poll_answer(self, question_id, timeout=55, interval=1):
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
                
            try:
                response = self.session.get(url, timeout=10)
                
                # If we get any response with status 200, mark first response time
                if response.status_code == 200:
                    first_response_time = time.time() - start_time
                    break
            except:
                pass  # Ignore errors during quick polling
                
            time.sleep(0.1)
        
        # If we still don't have first response time, set it equal to whenever we get the final answer
        if first_response_time is None:
            first_response_time = time.time() - start_time
            
        # Continue polling until completion
        while (time.time() - start_time) < timeout:
            try:
                response = self.session.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    # Check if we have a complete answer
                    if "answers" in data and len(data["answers"]) > 0:
                        # Consider it complete if it has answer text
                        if "text" in data["answers"][0] and data["answers"][0]["text"]:
                            self.last_activity = time.time()
                            return data, first_response_time, time.time() - start_time
                
                elif response.status_code != 202:
                    response.raise_for_status()
            except Exception as e:
                # Log error but continue polling
                print(f"Error during polling: {str(e)}")
        
            time.sleep(interval)
        
        raise TimeoutError(f"Polling timed out after {timeout} seconds")
        
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

# Function to create a downloadable CSV
def create_download_csv(df):
    # Create a CSV string from the DataFrame
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_string = csv_buffer.getvalue()
    
    # Return the CSV data
    return csv_string

# Function to save checkpoint
def save_checkpoint(progress_idx, df):
    st.session_state.questions_processed = progress_idx
    st.session_state.results_df = df.copy()

# Run queries function with checkpointing
def run_queries(questions_list, start_index=0):
    # Create or load results DataFrame with additional assessment columns
    if st.session_state.results_df is not None and start_index > 0:
        results_df = st.session_state.results_df
        st.info(f"Resuming from question {start_index+1}/{len(questions_list)}")
    else:
        results_df = pd.DataFrame(columns=[
            "Question",
            "Answer",
            "Insights",
            "Interpretation",
            "SQL",
            "API Response Time (seconds)",
            "Total Response Time (seconds)",
            "Estimated Time to First Response (seconds)",
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
    
    # Heartbeat element to keep connection alive
    heartbeat_text = st.empty()

    # Calculate initial progress percentage
    if start_index > 0:
        initial_progress = int((start_index / len(questions_list)) * 100)
    else:
        initial_progress = 0
    progress_bar.progress(initial_progress)

    # Calculate estimated time
    total_questions = len(questions_list)
    delay_between_questions = 15  # Reduced from 20 to improve throughput
    estimated_time = (total_questions - start_index) * (delay_between_questions + 15)
    st.info(f"Estimated time: {estimated_time//60} minutes {estimated_time%60} seconds")

    # Create a container for intermediate results
    results_container = st.container()
    
    # Create a container for downloaded results
    download_container = st.container()
    with download_container:
        dl_placeholder = st.empty()

    # Set processing as active
    st.session_state.processing_active = True

    # Process each question
    try:
        for i in range(start_index, len(questions_list)):
            question = questions_list[i]
            
            # Update heartbeat
            current_time = datetime.now().strftime("%H:%M:%S")
            heartbeat_text.info(f"Heartbeat: {current_time} - Processing question {i+1}/{total_questions}")
            
            # Update progress
            progress = int(((i + 1) / total_questions) * 100)
            progress_bar.progress(progress)
            status_text.text(f"Processing question {i+1}/{total_questions}: {question}")

            # Try to refresh token periodically
            if i > start_index and i % 3 == 0:
                try:
                    client.refresh_session(username, password)
                except:
                    pass  # Continue even if refresh fails

            try:
                # Submit question with retry logic
                question_id = client.submit_question(question, username, password)
                
                # Poll for answer with timing information (will timeout after 55 seconds)
                result, first_response_time, total_response_time = client.poll_answer(question_id)
                
                # Extract data
                answer_text = result["answers"][0]["text"] if "answers" in result and len(result["answers"]) > 0 else "No answer provided"
                interpretation, sql, insights = client.extract_data_from_response(result)
                
                # Analyze SQL complexity and get estimated latency
                latency, complexity = analyze_sql_complexity(sql)
                
                # Calculate estimated time to first response
                estimated_first_response = first_response_time + latency
                
                # Add to DataFrame with empty assessment columns
                results_df.loc[len(results_df)] = [
                    question,
                    answer_text,
                    insights,
                    interpretation,
                    sql,
                    round(first_response_time, 2),
                    round(total_response_time, 2),
                    round(estimated_first_response, 2),
                    "",  # Question Difficulty - left empty for user to fill
                    "",  # Pass/Fail - left empty for user to fill
                    ""   # Answer Accuracy - left empty for user to fill
                ]
                
                with results_container:
                    st.success(f"""✓ Got answer for question {i+1}:
                    - API Response Time: {first_response_time:.2f}s
                    - Total Response Time: {total_response_time:.2f}s
                    - SQL Complexity: {complexity} (+{latency:.1f}s)
                    - Estimated Time to First Response: {estimated_first_response:.2f}s""")
                
            except TimeoutError as e:
                # Handle timeout specifically
                with results_container:
                    st.warning(f"⚠️ Question {i+1} timed out after 55 seconds. Skipping to next question.")
                
                # Add timeout to DataFrame with empty assessment columns
                results_df.loc[len(results_df)] = [
                    question,
                    "TIMEOUT: Question processing exceeded 55 seconds",
                    "",
                    "",
                    "",
                    55,  # Set to timeout value
                    55,  # Set to timeout value
                    55,  # Set to timeout value
                    "",  # Question Difficulty
                    "Skip",  # Mark as skipped
                    ""   # Answer Accuracy
                ]
                
                # Try to refresh the session after a timeout
                try:
                    client.refresh_session(username, password)
                except:
                    pass  # Continue even if refresh fails
                
            except Exception as e:
                # Get detailed error information
                error_details = traceback.format_exc()
                with results_container:
                    st.error(f"Error processing question {i+1}: {str(e)}")
                    st.code(error_details, language="python")
                
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
                
                # Try to refresh the session after an error
                try:
                    client.refresh_session(username, password)
                except:
                    pass  # Continue even if refresh fails
            
            # Save checkpoint after each question
            save_checkpoint(i+1, results_df)
            
            # Update download link after each question
            with download_container:
                # Generate CSV file for download (includes all columns)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_data = create_download_csv(results_df)
                
                dl_placeholder.download_button(
                    label=f"📥 Download CSV Results ({i+1}/{total_questions} questions)",
                    data=csv_data,
                    file_name=f"bot_queries_{timestamp}.csv",
                    mime="text/csv",
                    key=f"download_btn_{i}_{timestamp}"
                )
                
                # Force Streamlit to redraw by introducing a small delay
                time.sleep(0.1)

            # Delay before next question - reduced delay for better efficiency
            if i < len(questions_list) - 1:
                status_text.text(f"Waiting {delay_between_questions} seconds before next question...")
                time.sleep(delay_between_questions)
    
    except Exception as e:
        # Handle any unexpected errors
        st.error(f"Unexpected error during processing: {str(e)}")
        st.code(traceback.format_exc(), language="python")
    
    finally:
        # Set processing as inactive
        st.session_state.processing_active = False
        
        # Update progress to 100%
        progress_bar.progress(100)
        status_text.text("All questions processed!")
        
        # Final save
        save_checkpoint(len(questions_list), results_df)

    return results_df

# Add custom CSS styling for your interface
st.markdown("""
<style>
.stButton > button { background-color: #4CAF50; color: white; font-size: 18px; padding: 10px 24px; border-radius: 8px; }
.stButton > button:hover { background-color: #45a049; }
.download-btn { background-color: #008CBA; }
.stProgress > div > div { background-color: #4CAF50; }
.right-align { text-align: right; width: 100%; }
</style>
""", unsafe_allow_html=True)

# Main app logic
if uploaded_file is not None:
    # Parse questions from the uploaded CSV
    questions_list = parse_questions_from_csv(uploaded_file)
    
    # Store questions in session state
    if questions_list and len(questions_list) > 0 and questions_list != st.session_state.questions_list:
        st.session_state.questions_list = questions_list
        st.session_state.questions_processed = 0  # Reset progress when questions change
        st.session_state.total_questions = len(questions_list)
    
    if st.session_state.questions_list:
        st.write(f"Found {len(st.session_state.questions_list)} questions in the CSV file")
        
        # Show first few questions
        if len(st.session_state.questions_list) > 0:
            st.subheader("Sample Questions:")
            for i, q in enumerate(st.session_state.questions_list[:5]):
                st.write(f"{i+1}. {q}")
            if len(st.session_state.questions_list) > 5:
                st.write("...")
        
        # Show progress if there's any
        if st.session_state.questions_processed > 0 and st.session_state.questions_processed < st.session_state.total_questions:
            st.info(f"Progress: {st.session_state.questions_processed}/{st.session_state.total_questions} questions processed")
            
            # Show partial results
            if st.session_state.results_df is not None:
                st.subheader("Current Results")
                display_df = st.session_state.results_df.drop(columns=["Question Difficulty (1-5)", "Pass/Fail", "Answer Accuracy (1-5)"])
                st.dataframe(display_df, use_container_width=True)
                
                # Generate CSV file for download
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_data = create_download_csv(st.session_state.results_df)
                
                # Create download button for intermediate results
                st.download_button(
                    label=f"📥 Download Partial Results ({st.session_state.questions_processed}/{st.session_state.total_questions} questions)",
                    data=csv_data,
                    file_name=f"bot_queries_partial_{timestamp}.csv",
                    mime="text/csv",
                    key="download_partial"
                )
        
        # Create buttons for running or resuming
        col1, col2 = st.columns(2)
        
        with col1:
            if st.session_state.processing_active:
                st.warning("Processing in progress... Please wait.")
            elif st.session_state.questions_processed > 0 and st.session_state.questions_processed < st.session_state.total_questions:
                resume_button = st.button("▶️ Resume Processing", key="resume_btn", 
                                        help=f"Resume from question {st.session_state.questions_processed+1}")
                restart_button = st.button("🔄 Start Over", key="restart_btn")
                
                if resume_button:
                    if not username or not password:
                        st.error("Please enter your username and password")
                    else:
                        # Run queries and resume from checkpoint
                        results_df = run_queries(st.session_state.questions_list, st.session_state.questions_processed)
                
                if restart_button:
                    # Reset progress
                    st.session_state.questions_processed = 0
                    st.session_state.results_df = None
                    st.experimental_rerun()
            else:
                run_button = st.button("▶️ Run Queries", key="run_btn")
                
                if run_button:
                    if not username or not password:
                        st.error("Please enter your username and password")
                    else:
                        # Run queries from the beginning
                        results_df = run_queries(st.session_state.questions_list)
                        
                        if results_df is not None and len(results_df) == st.session_state.total_questions:
                            # Display final results (hide assessment columns in the display)
                            display_df = results_df.drop(columns=["Question Difficulty (1-5)", "Pass/Fail", "Answer Accuracy (1-5)"])
                            st.subheader("Final Results")
                            st.dataframe(display_df, use_container_width=True)
                            
                            # Generate CSV file for download (includes all columns)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            csv_data = create_download_csv(results_df)
                            
                            # Create download button for CSV with custom styling
                            st.download_button(
                                label="📥 Download CSV Results (includes assessment columns)",
                                data=csv_data,
                                file_name=f"bot_queries_{timestamp}.csv",
                                mime="text/csv",
                                key="download_final"
                            )
                            
                            # Add note about assessment columns
                            st.info("The downloaded CSV includes additional columns for manual assessment: 'Question Difficulty (1-5)', 'Pass/Fail', and 'Answer Accuracy (1-5)'.")
    else:
        st.error("No questions found in the CSV file. Please make sure the file contains questions.")
else:
    st.info("Please upload a CSV file with questions to continue.")
