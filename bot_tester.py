import requests
import time
import json
import os
import pandas as pd
from datetime import datetime


class ChatbotClient:
    def __init__(self, base_url, bot_id, project_id):
        self.base_url = base_url
        self.bot_id = bot_id
        self.project_id = project_id
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def login(self, username, password):
        """Authenticate and store token in session headers"""
        print("Logging in...")
        url = f"{self.base_url}/api/auth/login"
        payload = {
            "username": username,
            "password": password
        }

        response = self.session.post(url, json=payload)
        try:
            response.raise_for_status()  # Will throw error for non-204 status
            print("Login successful")
        except requests.exceptions.HTTPError as e:
            print(f"Login failed: {e}")
            exit(1)

        # Extract token from headers
        auth_token = response.headers.get("X-MSTR-AuthToken")
        if auth_token:
            self.session.headers.update({"X-MSTR-AuthToken": auth_token})

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
            "history": []  # No history needed
        }

        response = self.session.post(url, headers=headers, json=payload)
        if response.status_code == 401:
            self.login(*CREDS)
            response = self.session.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()["id"]

    def poll_answer(self, question_id, timeout=300, interval=1):
        """Poll for answer until ready or timeout"""
        start_time = time.time()
        url = f"{self.base_url}/api/questions/{question_id}"

        while (time.time() - start_time) < timeout:
            response = self.session.get(url)
            if response.status_code == 401:
                self.login(*CREDS)
                response = self.session.get(url)

            if response.status_code == 200:
                return response.json(), time.time() - start_time  # Return response and time taken
            elif response.status_code != 202:
                response.raise_for_status()

            time.sleep(interval)

        raise TimeoutError("Polling timed out after 5 minutes")

    def extract_interpretation_and_sql(self, response_data):
        """
        Extract both interpretation text and SQL queries from the response
        """
        interpretation = ""
        sql = ""

        # Get SQL from sqlQueries field
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


if __name__ == "__main__":
    # Configuration - replace with actual values
    BASE_URL = "https://autotrial.microstrategy.com/MicroStrategyLibrary"
    BOT_ID = "1DC776FB20744B85AFEE148D7C11C842"
    PROJECT_ID = "205BABE083484404399FBBA37BAA874A"
    CREDS = ("skytouch_ahuffman", "4%PAafAM6kdp")

    # Output file name (Excel)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    EXCEL_FILE = f"bot_queries_{timestamp}.xlsx"

    # List of questions to ask automatically
    QUESTIONS_LIST = [
        "What is the ABV of wines from Brazil?",
        "What types of wine pair well with fish?",
        "Which wine types have the highest average rating?",
        "What are the top 5 wines by Acidity?",
        "What is the difference between a French wine and a Portuguese wine?",
        "Which country produces the most Merlot?",
        "What attributes are available in the dataset?",
        "Give me a list of Chardonnays from the United Sates including their body and average rating.",
        "What types of grapes generally produce higher ABV?",
        "How many wines are produced by the Amaranta winery?",
        "Which wines from Amaranta have the highest ABV?",
        "Give me the top 10 American Cabernet Sauvignons by average rating.",
        "Show me a ring chart of all wines broken by grape variety."
    ]

    # Create a data frame to store results
    results_df = pd.DataFrame(columns=[
        "Question",
        "Answer",
        "Interpretation",
        "SQL",
        "Response Time (seconds)"
    ])

    # Create a directory for storing response data (if needed)
    os.makedirs("response_data", exist_ok=True)

    # Sample execution flow
    client = ChatbotClient(BASE_URL, BOT_ID, PROJECT_ID)

    # 1. Login
    client.login(*CREDS)

    # 2. Calculate and display estimated runtime
    delay_between_questions = 20  # seconds
    estimated_runtime = len(QUESTIONS_LIST) * (
                delay_between_questions + 10)  # Adding 10 seconds per question for processing
    estimated_minutes = estimated_runtime / 60

    print(f"Estimated runtime: {estimated_minutes:.1f} minutes for {len(QUESTIONS_LIST)} questions")
    print(f"Results will be saved to {EXCEL_FILE}")
    print("-" * 50)

    # 3. Loop through predefined questions
    for i, question_text in enumerate(QUESTIONS_LIST):
        print(f"Processing question {i + 1}/{len(QUESTIONS_LIST)}: {question_text}")

        try:
            # Submit question and record start time
            start_time = time.time()
            question_id = client.submit_question(question_text)

            # Poll for results
            result, response_time = client.poll_answer(question_id)

            # Get answer text
            answer_text = result["answers"][0]["text"] if "answers" in result and len(
                result["answers"]) > 0 else "No answer provided"

            # Extract interpretation and SQL
            interpretation, sql = client.extract_interpretation_and_sql(result)

            # Add to dataframe
            results_df.loc[len(results_df)] = [
                question_text,
                answer_text,
                interpretation,
                sql,
                round(response_time, 2)
            ]

            print(f"  ✓ Got answer in {response_time:.2f} seconds")

        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            # Add error entry to dataframe
            results_df.loc[len(results_df)] = [
                question_text,
                f"ERROR: {str(e)}",
                "",
                "",
                0
            ]

        # Add a delay between questions to avoid overwhelming the API
        if i < len(QUESTIONS_LIST) - 1:  # No need to delay after the last question
            print(f"  Waiting {delay_between_questions} seconds before next question...")
            time.sleep(delay_between_questions)

    # Save results to Excel
    print("-" * 50)
    print(f"Saving results to {EXCEL_FILE}...")

    # Format the Excel file
    with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='Bot Queries', index=False)

        # Auto-adjust column widths
        worksheet = writer.sheets['Bot Queries']
        for i, col in enumerate(results_df.columns):
            max_length = max(
                results_df[col].astype(str).map(len).max(),
                len(col)
            )
            # Limiting width to avoid extremely wide columns
            adjusted_width = min(max_length + 2, 100)
            worksheet.column_dimensions[chr(65 + i)].width = adjusted_width

    print(f"Done! Results saved to {EXCEL_FILE}")
