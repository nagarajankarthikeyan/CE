# CE AI Analytics

AI-powered marketing analytics platform built with:

-   Angular 21 (Standalone)
-   FastAPI
-   OpenAI
-   Google BigQuery
-   Server-Sent Events (Streaming UI)

------------------------------------------------------------------------

# Architecture Overview

Angular (4200) ↓ FastAPI (8000) ↓ BigQuery (Analytics + Users + Audit) ↓
OpenAI (SQL + Narrative Generation)

------------------------------------------------------------------------

# Prerequisites

## 1. Node.js (v18+ recommended)

https://nodejs.org/

Verify: node -v npm -v

------------------------------------------------------------------------

## 2. Angular CLI (v21.1.0)

npm install -g @angular/cli

Verify: ng version

------------------------------------------------------------------------

## 3. Python 3.11+

Verify: python --version

------------------------------------------------------------------------

## 4. Create Virtual Environment

python -m venv venv venv`\Scripts`{=tex}`\activate`{=tex}

------------------------------------------------------------------------

# Backend Setup (FastAPI + BigQuery)

## Install Dependencies

Path: backend/

Create venv:
python -m venv venv
venv\Scripts\activate

Install:
pip install -r requirements.txt

pip install fastapi uvicorn openai google-cloud-bigquery
passlib\[bcrypt\] python-dotenv

Run:
uvicorn app.main:app --reload


------------------------------------------------------------------------

# BigQuery Setup

Project: bounteous-bi

Dataset: constellation_media_AI_ANALYST

Tables: - complete_constellation (Analytics View) - users
(Authentication) - AuditLogs (Audit logging)

Location: US

------------------------------------------------------------------------

# Service Account Setup

1.  Go to Google Cloud Console → IAM & Admin → Service Accounts
2.  Create Service Account
3.  Grant roles:
    -   BigQuery Data Viewer
    -   BigQuery Job User
    -   BigQuery Data Editor
4.  Create JSON key and save locally (example:
    C:`\gcp`{=tex}`\bigquery`{=tex}-key.json)

------------------------------------------------------------------------

# Environment Variables (Windows)

set
GOOGLE_APPLICATION_CREDENTIALS=C:`\gcp`{=tex}`\bigquery`{=tex}-key.json
set OPENAI_API_KEY=your_openai_key

------------------------------------------------------------------------

# Backend Configuration (config.py)

BIGQUERY_PROJECT=bounteous-bi
BIGQUERY_DATASET=constellation_media_AI_ANALYST
BIGQUERY_VIEW=complete_constellation BIGQUERY_LOCATION=US

------------------------------------------------------------------------

# Run Backend

uvicorn backend.app.main:app --reload

Backend URL: http://localhost:8000

------------------------------------------------------------------------

# Authentication Flow

Frontend sends Base64(email:password). Backend validates user from
BigQuery users table. Returns 401 if invalid.

------------------------------------------------------------------------

# Streaming (SSE)

Endpoint: GET /chat/stream

Returns: event: render data: {render_spec}

data: narrative tokens

event: done

------------------------------------------------------------------------

# Audit Logging

Logs: - conversation_id - generated_sql - rows_returned -
execution_time - status - error details - CreatedAt (UTC)

------------------------------------------------------------------------

# Frontend Setup

ng serve

Open: http://localhost:4200

------------------------------------------------------------------------

# Build Frontend

ng build

------------------------------------------------------------------------

# Features

-   ChatGPT-style UI
-   Streaming narrative
-   KPI / Table / Chart / Ranked List / Mixed rendering
-   Currency formatting (\$ with 2 decimals)
-   Percent formatting
-   Dynamic column handling
-   Basic authentication
-   Audit logging

------------------------------------------------------------------------

# Common Issues

## 403 Access Denied

Ensure BigQuery roles are granted.

## 404 Table Not Found

Check dataset location (US).

## CORS Error

Ensure FastAPI CORSMiddleware allows http://localhost:4200

------------------------------------------------------------------------

# Run Order

Terminal 1: venv`\Scripts`{=tex}`\activate`{=tex} uvicorn
backend.app.main:app --reload

Terminal 2: ng serve

------------------------------------------------------------------------

Frontend: http://localhost:4200

Backend: http://localhost:8000
