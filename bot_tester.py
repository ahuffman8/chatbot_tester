import streamlit as st
import pandas as pd
import requests
import time
import io
import csv
import re
from datetime import datetime
import threading

# Page configuration
st.set_page_config(
    page_title="Strategy Bot Query Tool",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state
if 'results' not in st.session_state:
    st.session_state.results = []
if 'processing' not in st.session_state:
    st.session_state.processing = False
if 'completed' not in st.session_state:
    st.session_state.completed = False
if 'processed_count' not in st.session_state:
    st.session_state.processed_count = 0
if 'total_count' not in st.session_state:
    st.session_state.total_count = 0

# App title and description
st.title("Strategy Bot Query Tool")

# Instructions
st.markdown("""
Use this site to send multiple queries to a bot and test for accuracy. Add your bot information and credentials,
then attach a CSV file with all the questions you want to ask and click "Run Queries".
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
    workers = st.slider("Workers", min_value=1, max_value=5, value=3)

with input_col2:
    # Authentication
    st.subheader("Authentication")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

# File upload
st.subheader("Questions File")
uploaded_file = st.file_uploader("Upload CSV file with questions", type="csv")

# Parse questions function
def parse_questions_from_csv(file):
    questions = []
    file.seek(0)
    try:
        df = pd.read_csv(file)
        # Use first column
        if len(df.columns) > 0:
            questions = df.iloc[:,0].dropna().tolist()
    except:
        try:
            # Try simple CSV parsing
            file.seek(0)
            text_io = io.TextIOWrapper(file, encoding='utf8')
            reader = csv.reader(text_io)
            for row in reader:
                if row and row[0].strip():
                    questions.append(row[0].strip())
        except Exception as e:
            st.error(f"Error parsing CSV: {e}")
    
    return questions

# Simple function to process a single question
def process_question(question, base_url, project_id, bot_id, username, password):
    try:
        # Create session
        session = requests.Session()
        session.headers.update({"Content-Type": "application/json"})
        
        # Login
        login_url = f"{base_url}/api/auth/login"
        login_payload = {"username": username, "password": password}
        login_response = session.post(login_url, json=login_payload)
        login_response.raise_for_status()
        
        # Get auth token
        auth_token = login_response.headers.get("X-MSTR-AuthToken")
        if not auth_token:
            return {"question": question, "answer": "ERROR: Login failed", "status": "failed"}
        
        session.headers.update({"X-MSTR-AuthToken": auth_token})
        
        # Submit question
        question_url = f"{base_url}/api/questions"
        question_headers = {"Prefer": "respond-async", "X-MSTR-ProjectID": project_id}
        question_payload = {
            "text": question,
            "textOnly": True,
            "bots": [{"id": bot_id, "projectId": project_id}],
            "history": []
        }
        
        question_response = session.post(question_url, headers=question_headers, json=question_payload)
        question_response.raise_for_status()
        question_id = question_response.json()["id"]
        
        # Poll for answer
        start_time = time.time()
        poll_url = f"{base_url}/api/questions/{question_id}"
        answer_data = None
        
        # Poll for up to 5 minutes
        while time.time() - start_time < 300:
            poll_response = session.get(poll_url)
            
            if poll_response.status_code == 200:
                answer_data = poll_response.json()
                if "answers" in answer_data and len(answer_data["answers"]) > 0:
                    if "text" in answer_data["answers"][0] and answer_data["answers"][0]["text"]:
                        break
            elif poll_response.status_code != 202:
                return {"question": question, "answer": f"ERROR: Poll failed with code {poll_response.status_code}", "status": "failed"}
            
            time.sleep(1)
            
        if not answer_data or "answers" not in answer_data or len(answer_data["answers"]) == 0:
            return {"question": question, "answer": "ERROR: No answer received after timeout", "status": "failed"}
            
        # Extract answer text
        answer_text = answer_data["answers"][0].get("text", "No text in answer")
        
        # Extract SQL if available
        sql = ""
        if "answers" in answer_data and len(answer_data["answers"]) > 0:
            if "sqlQueries" in answer_data["answers"][0] and len(answer_data["answers"][0]["sqlQueries"]) > 0:
                sql = answer_data["answers"][0]["sqlQueries"][0]
        
        # Extract interpretation if available
        interpretation = ""
        if "answers" in answer_data and len(answer_data["answers"]) > 0:
            if "queries" in answer_data["answers"][0] and len(answer_data["answers"][0]["queries"]) > 0:
                query = answer_data["answers"][0]["queries"][0]
                if "explanation" in query:
                    interpretation = query["explanation"]
        
        # Extract insights if available
        insights = ""
        if "answers" in answer_data and len(answer_data["answers"]) > 0:
            answer = answer_data["answers"][0]
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
        
        # Return successful result
        return {
            "question": question,
            "answer": answer_text,
            "sql": sql,
            "interpretation": interpretation,
            "insights": insights,
            "time": round(time.time() - start_time, 2),
            "status": "success"
        }
    
    except Exception as e:
        return {"question": question, "answer": f"ERROR: {str(e)}", "status": "failed"}

# Background worker function
def background_worker(questions, base_url, project_id, bot_id, username, password):
    st.session_state.processing = True
    st.session_state.processed_count = 0
    st.session_state.total_count = len(questions)
    st.session_state.results = []
    
    # Process each question
    for q in questions:
        result = process_question(q, base_url, project_id, bot_id, username, password)
        
        # Update session state
        st.session_state.results.append(result)
        st.session_state.processed_count += 1
        
        # Add a small delay to avoid overwhelming the server
        time.sleep(1)
    
    # Mark as completed
    st.session_state.completed = True
    st.session_state.processing = False

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
        
        # Create buttons
        col1, col2 = st.columns(2)
        
        with col1:
            run_button_pressed = st.button("▶️ Run Queries", key="run_btn", 
                                          disabled=st.session_state.processing)
            
        with col2:
            reset_button_pressed = st.button("🔄 Reset", key="reset_btn")
        
        # Show progress
        if st.session_state.processing or st.session_state.completed:
            progress_pct = int((st.session_state.processed_count / st.session_state.total_count) * 100) if st.session_state.total_count > 0 else 0
            st.progress(progress_pct)
            st.write(f"Processed {st.session_state.processed_count} of {st.session_state.total_count} questions")
        
        # Handle reset
        if reset_button_pressed:
            st.session_state.results = []
            st.session_state.processing = False
            st.session_state.completed = False
            st.session_state.processed_count = 0
            st.session_state.total_count = 0
            st.experimental_rerun()
        
        # Handle run button
        if run_button_pressed:
            if not username or not password:
                st.error("Please enter your username and password")
            else:
                # Start the background processing
                thread = threading.Thread(
                    target=background_worker,
                    args=(questions_list, base_url, project_id, bot_id, username, password)
                )
                thread.daemon = True
                thread.start()
                st.info("Processing started in the background. This page will update automatically.")
                time.sleep(1)  # Give the thread time to start
                st.experimental_rerun()
        
        # Show download button when completed
        if st.session_state.completed and st.session_state.results:
            st.success("✅ All questions have been processed successfully!")
            
            # Create DataFrame from results
            results_df = pd.DataFrame([
                {
                    "Question": r["question"],
                    "Answer": r["answer"],
                    "Insights": r.get("insights", ""),
                    "Interpretation": r.get("interpretation", ""),
                    "SQL": r.get("sql", ""),
                    "Response Time (seconds)": r.get("time", 0),
                    "Question Difficulty (1-5)": "",
                    "Pass/Fail": "Fail" if r["status"] == "failed" else "",
                    "Answer Accuracy (1-5)": ""
                }
                for r in st.session_state.results
            ])
            
            # Create downloadable CSV
            csv_buffer = io.StringIO()
            results_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            # Show the download button
            st.download_button(
                label=f"📥 Download Results CSV ({len(results_df)} questions)",
                data=csv_data,
                file_name=f"bot_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_btn"
            )
            
            # Also show results preview
            st.subheader("Results Preview")
            st.dataframe(results_df.head(10))
        
        # Show partial results during processing
        elif st.session_state.processing and st.session_state.results:
            # Show current results
            st.subheader(f"Partial Results ({len(st.session_state.results)} questions processed so far)")
            
            # Create DataFrame from current results
            partial_df = pd.DataFrame([
                {
                    "Question": r["question"],
                    "Answer": r["answer"],
                    "Status": r["status"],
                    "Time": r.get("time", 0)
                }
                for r in st.session_state.results
            ])
            
            st.dataframe(partial_df)
    else:
        st.error("No questions found in the CSV file. Please make sure the file contains questions.")
else:
    st.info("Please upload a CSV file with questions to continue.")
