import streamlit as st
import pandas as pd
import requests
import time
import io
import csv
from datetime import datetime

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
We'll let you know how long the process will take. When you come back you'll be able to 
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
        """Poll for answer until ready or timeout"""
        start_time = time.time()
        url = f"{self.base_url}/api/questions/{question_id}"

        while (time.time() - start_time) < timeout:
            response = self.session.get(url)
            if response.status_code == 200:
                return response.json(), time.time() - start_time
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

# Parse the CSV file to extract questions
def parse_questions_from_csv(file):
    questions = []
    
    # Reset file pointer to beginning
    file.seek(0)
    
    try:
        # First, try reading as a simple CSV without assuming headers
        csv_reader = csv.reader(file)
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
            csv_data = pd.read_csv(file, header=None)
            questions = csv_data[0].dropna().tolist()
        except Exception as inner_e:
            st.error(f"Fallback CSV reading also failed: {str(inner_e)}")
    
    return questions

# Run queries function
def run_queries(questions_list):
    # Create results DataFrame
    results_df = pd.DataFrame(columns=[
        "Question", 
        "Answer", 
        "Interpretation", 
        "SQL", 
        "Response Time (seconds)"
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
            
            # Add to DataFrame
            results_df.loc[len(results_df)] = [
                question,
                answer_text,
                interpretation,
                sql,
                round(response_time, 2)
            ]
            
            # Show intermediate result
            st.success(f"✓ Got answer for question {i+1} in {response_time:.2f} seconds")
            
        except Exception as e:
            st.error(f"Error processing question {i+1}: {str(e)}")
            
            # Add error to DataFrame
            results_df.loc[len(results_df)] = [
                question,
                f"ERROR: {str(e)}",
                "",
                "",
                0
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
</style>
""", unsafe_allow_html=True)

# Main app logic
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
        
        # Run button with custom styling
        if st.button("▶️ Run Queries", key="run_btn"):
            if not username or not password:
                st.error("Please enter your username and password")
            else:
                # Run queries and get results
                results_df = run_queries(questions_list)
                
                if results_df is not None:
                    # Display results
                    st.subheader("Results")
                    st.dataframe(results_df, use_container_width=True)
                    
                    # Generate CSV file for download
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv_data = create_download_csv(results_df)
                    
                    # Create download button for CSV with custom styling
                    st.markdown('<div class="download-section">', unsafe_allow_html=True)
                    st.download_button(
                        label="📥 Download CSV Results",
                        data=csv_data,
                        file_name=f"bot_queries_{timestamp}.csv",
                        mime="text/csv",
                        key="download_btn"
                    )
                    st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.error("No questions found in the CSV file. Please make sure the file contains questions.")
else:
    st.info("Please upload a CSV file with questions to continue.")
