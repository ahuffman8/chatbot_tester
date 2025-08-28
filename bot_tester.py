import streamlit as st
import pandas as pd
import requests
import time
import io
import csv
from datetime import datetime

# Check for Excel dependencies
excel_support = True
try:
    import openpyxl
except ImportError:
    excel_support = False
    st.warning("Excel support is not available. Install the 'openpyxl' package using pip: `pip install openpyxl`")

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
then attach a CSV or Excel file with all the questions you want to ask and then click "Run Queries". 
We'll let you know how long the process will take and then when you come back you'll be able to 
download a file with all the questions, answers, interpretations, SQL queries, and response times to judge the performance of your bot.
""")

# Fixed AI generation speed (characters per second) - not user configurable
AI_GENERATION_SPEED = 70  # Characters per second

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
    
    # File upload - handle Excel based on dependency availability
    st.subheader("Questions File")
    if excel_support:
        uploaded_file = st.file_uploader("Upload CSV or Excel file with questions", type=["csv", "xlsx", "xls"])
    else:
        uploaded_file = st.file_uploader("Upload CSV file with questions", type=["csv"])
        st.info("To enable Excel file uploads, install the 'openpyxl' package: `pip install openpyxl`")

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
        """Ultra-simplified polling method that just waits for completion"""
        start_time = time.time()
        url = f"{self.base_url}/api/questions/{question_id}"
        
        while (time.time() - start_time) < timeout:
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check if the response is complete
                if ("answers" in data and len(data["answers"]) > 0 and 
                    "status" in data["answers"][0] and data["answers"][0]["status"] == "completed"):
                    
                    return data, time.time() - start_time
            
            elif response.status_code != 202:
                response.raise_for_status()
            
            time.sleep(interval)

        raise TimeoutError("Polling timed out after 5 minutes")
        
    def extract_interpretation_and_sql(self, response_data):
        """Extract both interpretation text and SQL queries from the response"""
        interpretation = ""
        sql = ""
        
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
        
        return interpretation, sql

# Parse questions from the uploaded file
def parse_questions_from_file(file):
    questions = []
    
    # Reset file pointer to beginning
    file.seek(0)
    
    # Get file type from name
    file_name = file.name.lower()
    
    try:
        if file_name.endswith('.csv'):
            # Handle CSV file
            try:
                # Read as pandas DataFrame first (more reliable)
                file.seek(0)
                csv_data = pd.read_csv(file)
                
                # If we have at least one column, take the first column
                if len(csv_data.columns) > 0:
                    col = csv_data.columns[0]
                    questions = csv_data[col].dropna().tolist()
                    
            except Exception as e:
                st.error(f"Error reading CSV file with pandas: {str(e)}")
                # Fallback to simpler method
                file.seek(0)
                content = file.read().decode('utf-8').splitlines()
                questions = [line.strip() for line in content if line.strip()]
                
        elif file_name.endswith(('.xlsx', '.xls')) and excel_support:
            # Handle Excel file only if openpyxl is available
            try:
                excel_data = pd.read_excel(file)
                
                # If we have at least one column, take the first column
                if len(excel_data.columns) > 0:
                    col = excel_data.columns[0]
                    questions = excel_data[col].dropna().tolist()
                    
            except Exception as e:
                st.error(f"Error reading Excel file: {str(e)}")
                if "openpyxl" in str(e):
                    st.error("Missing dependency 'openpyxl'. Please install it using: pip install openpyxl")
                
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
    
    return questions

# Run queries function
def run_queries(questions_list):
    # Create results DataFrame
    results_df = pd.DataFrame(columns=[
        "Question", 
        "Answer", 
        "Interpretation", 
        "SQL", 
        "Response Time (seconds)",
        "Estimated Start Time (seconds)",
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
            
            # Poll for answer
            result, response_time = client.poll_answer(question_id)
            
            # Extract data
            answer_text = result["answers"][0]["text"] if "answers" in result and len(result["answers"]) > 0 else "No answer provided"
            interpretation, sql = client.extract_interpretation_and_sql(result)
            
            # Calculate estimated start time based on text length and generation speed
            text_length = len(answer_text)
            generation_time = text_length / AI_GENERATION_SPEED  # Using fixed speed
            
            # If generation time > response time, use a reasonable estimate
            if generation_time >= response_time:
                estimated_start_time = response_time * 0.1  # Assume thinking took 10% of total time
            else:
                estimated_start_time = response_time - generation_time
            
            # Add to DataFrame
            results_df.loc[len(results_df)] = [
                question,
                answer_text,
                interpretation,
                sql,
                round(response_time, 2),
                round(estimated_start_time, 2),
                "",  # Question Difficulty - left empty for user to fill
                "",  # Pass/Fail - left empty for user to fill
                ""   # Answer Accuracy - left empty for user to fill
            ]
            
            # Show intermediate result
            st.success(f"✓ Got answer for question {i+1} in {response_time:.2f}s (Est. start: {estimated_start_time:.2f}s)")
            
        except Exception as e:
            st.error(f"Error processing question {i+1}: {str(e)}")
            
            # Add error to DataFrame with empty assessment columns
            results_df.loc[len(results_df)] = [
                question,
                f"ERROR: {str(e)}",
                "",
                "",
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

# Add custom CSS styling for your interface, including positioned run button
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
    
    /* Fixed position run button container */
    .fixed-run-button {
        position: fixed;
        bottom: 20px;
        right: 20px;
        z-index: 100;
    }
    
    /* Make the button larger and more prominent */
    .fixed-run-button button {
        font-size: 20px !important;
        padding: 12px 28px !important;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* Add a small animation on hover */
    .fixed-run-button button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.3);
    }
</style>
""", unsafe_allow_html=True)

# Main app logic
if uploaded_file is not None:
    # Parse questions from the uploaded file (now supports Excel)
    questions_list = parse_questions_from_file(uploaded_file)
    
    if questions_list:
        st.write(f"Found {len(questions_list)} questions in the uploaded file")
        
        # Show first few questions
        if len(questions_list) > 0:
            st.subheader("Sample Questions:")
            for i, q in enumerate(questions_list[:5]):
                st.write(f"{i+1}. {q}")
            if len(questions_list) > 5:
                st.write("...")
        
        # Create results display area (will be populated after running queries)
        results_container = st.container()
        
        # Add spacer to ensure content isn't hidden behind the fixed button
        st.markdown("<div style='height: 100px;'></div>", unsafe_allow_html=True)
else:
    st.info("Please upload a CSV" + (" or Excel" if excel_support else "") + " file with questions to continue.")
    questions_list = []

# Fixed position run button at the bottom right
# This is outside the if-block so it's always shown, but disabled if no file is uploaded
st.markdown('<div class="fixed-run-button">', unsafe_allow_html=True)
run_button_clicked = st.button(
    "▶️ Run Queries", 
    key="run_btn", 
    disabled=(not questions_list or not uploaded_file)
)
st.markdown('</div>', unsafe_allow_html=True)

# Handle button click - this needs to be outside the HTML/CSS elements
if run_button_clicked:
    if not username or not password:
        st.error("Please enter your username and password")
    else:
        # Run queries and get results
        results_df = run_queries(questions_list)
        
        if results_df is not None:
            # Display results (hide assessment columns in the display)
            display_df = results_df.drop(columns=["Question Difficulty (1-5)", "Pass/Fail", "Answer Accuracy (1-5)"])
            with results_container:
                st.subheader("Results")
                st.dataframe(display_df, use_container_width=True)
                
                # Generate CSV file for download (includes all columns)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_data = create_download_csv(results_df)
                
                # Create download button for CSV with custom styling
                st.markdown('<div class="download-section">', unsafe_allow_html=True)
                st.download_button(
                    label="📥 Download CSV Results (includes assessment columns)",
                    data=csv_data,
                    file_name=f"bot_queries_{timestamp}.csv",
                    mime="text/csv",
                    key="download_btn"
                )
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Add note about assessment columns
                st.info("The downloaded CSV includes additional columns for manual assessment: 'Question Difficulty (1-5)', 'Pass/Fail', and 'Answer Accuracy (1-5)'.")
