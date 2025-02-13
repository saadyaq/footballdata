Football Data Engineering

This project is designed to crawl football-related data from Wikipedia using Apache Airflow, process and clean it, and store it for further analysis.
📌 Table of Contents

    System Architecture
    Requirements
    Getting Started
    Running the Code With Docker
    How It Works
    Project Overview
    Video (Optional, if you have one)

🏗 System Architecture

System Architecture
(Add a relevant architecture diagram if available)
⚙ Requirements

To run this project, you need the following dependencies:

    Python 3.9+
    Docker
    PostgreSQL
    Apache Airflow 2.6+

🚀 Getting Started

    Clone the repository

git clone https://github.com/saadyaq/footballdata.git
cd footballdata

Install Python dependencies

    pip install -r requirements.txt

🐳 Running the Code With Docker

    Start your services on Docker

    docker compose up -d

    Trigger the DAG on the Airflow UI
        Open Airflow in your browser (http://localhost:8080)
        Enable and trigger the DAG

🔍 How It Works

The project follows these main steps:

    Fetches football data from Wikipedia
    Cleans and preprocesses the data
    Transforms the dataset into a structured format
    Stores the cleaned data in a database

