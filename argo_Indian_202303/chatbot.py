import os
import streamlit as st
from sqlalchemy import create_engine
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.base import create_sql_agent

# ------------------------------
# DATABASE & LLM SETUP
# ------------------------------
DB_USER = st.secrets["db_username"]
DB_PASS = st.secrets["db_password"]
GOOGLE_API_KEY = st.secrets["google_api_key"]
DB_HOST = "localhost"
DB_NAME = "argo_data"

# ------------------------------
# DATABASE CONNECTION
# ------------------------------
try:
    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
    db = SQLDatabase(engine)
except Exception as e:
    st.error(f"Failed to connect to the database. Please check your credentials in the secrets.toml file. Error: {e}")
    st.stop()

# ------------------------------
# LANGCHAIN LLM + SQL AGENT
# ------------------------------
try:
    # Initialize the Gemini LLM
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=GOOGLE_API_KEY)


    # Create the SQL Agent
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        verbose=True
    )
except Exception as e:
    st.error(f"Failed to initialize the Language Model. Is your Google API Key correct in secrets.toml? Error: {e}")
    st.stop()

# ------------------------------
# STREAMLIT APP
# ------------------------------
st.title("🌊 FloatChat - ARGO Data Assistant")

user_query = st.text_input("Ask me about ARGO data (e.g., 'What is the average temperature for float 5904297?')")

if user_query:
    with st.spinner('Thinking...'):
        try:
            response = agent_executor.invoke({"input": user_query})
            answer = response.get("output", "Sorry, I couldn't find an answer.")
            st.write("**Answer:**")
            st.markdown(answer)
        except Exception as e:
            st.error(f"An error occurred while processing your query: {e}")
