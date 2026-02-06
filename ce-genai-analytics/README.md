README.md - CE AI Analytics

# CE AI Analytics

AI-powered chat + business intelligence over SQL Server JSON data using FastAPI, OpenAI, and Angular.

This project enables business users to ask natural language questions about marketing data and receive:

- KPIs
- Rankings
- Trend charts
- Executive summaries
- Narrative insights

## Architecture

Angular Chat UI → FastAPI Backend → SQL Server(BIG Query later)  
                                → OpenAI API

## Features

- Natural language → SQL
- Streaming responses (Server-Sent Events)
- KPI cards
- Ranked lists + bar charts
- Time-series trend charts
- Executive summary (mixed view)
- JSON-based dynamic schema
- Follow-up questions (context aware)
- ChatGPT-style UI

## Prerequisites

### Python 3.10+
Download: https://www.python.org/downloads/

Verify:
python --version

### Node.js 18+
Download: https://nodejs.org/

Verify:
node --version

### Angular CLI
npm install -g @angular/cli

Verify:
ng version

### SQL Server
Install SQL Server Express or Developer Edition

### ODBC Driver 17 for SQL Server
Required for pyodbc

## Database Setup

Database: CEAnalytics

Table:
CREATE TABLE DataLakeRaw (
    Id INT IDENTITY PRIMARY KEY,
    SourceSystem NVARCHAR(100),
    IngestedAt DATETIME2,
    FileName NVARCHAR(255),
    RecordIndex INT,
    RawJson NVARCHAR(MAX)
);

CREATE TABLE Users (
    UserID INT IDENTITY(1,1) PRIMARY KEY,
    Email NVARCHAR(255),
    Username NVARCHAR(100) ,
    PasswordHash NVARCHAR(255) ,
    IsActive BIT  DEFAULT 1,
    CreatedAt DATETIME2
);

CREATE TABLE AuditLogs (
    AuditID BIGINT IDENTITY(1,1) PRIMARY KEY,
    ConversationID NVARCHAR(100) NOT NULL,
    EventType NVARCHAR(50) NOT NULL,
    UserID INT,
    UserMessage NVARCHAR(MAX),
    GeneratedSQL NVARCHAR(MAX),
    SQLStatus NVARCHAR(50) NULL,
    RowsReturned INT,
    DurationMS BIGINT,
    ErrorType NVARCHAR(100),
    ErrorMessage NVARCHAR(MAX),
    Endpoint NVARCHAR(MAX),
    HTTPMethod NVARCHAR(10),
	Response NVARCHAR(MAX),
    ResponseStatus INT,
    ResponseDurationMS BIGINT,
    CreatedAt DATETIME2 DEFAULT GETUTCDATE(),
);

## Backend Setup

Path: backend/

Create venv:
python -m venv venv
venv\Scripts\activate

Install:
pip install -r requirements.txt

Run:
uvicorn app.main:app --reload

## Frontend Setup

Path: frontend/

npm install
ng serve

Frontend URL:
http://localhost:4200

Backend URL:
http://localhost:8000

## Demo Questions

- What is the total spend?
- Total spend by campaign
- Show spend by date
- Summarize Q4 2025 performance

## Streaming

Uses Server-Sent Events (SSE)



