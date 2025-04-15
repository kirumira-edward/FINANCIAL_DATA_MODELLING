@echo off
REM Windows scheduling script for financial data pipeline

REM Change to project directory
cd C:\Users\OPTIMUS\financial_data_modelling

REM Activate Python virtual environment (if used)
REM call venv\Scripts\activate.bat

REM Run the pipeline
python run_financial_pipeline.py

REM Exit with the same error code as the Python script
exit /b %ERRORLEVEL%

REM To schedule with Windows Task Scheduler:
REM 1. Open Task Scheduler
REM 2. Create a Basic Task
REM 3. Set trigger to Daily (e.g., 2:00 AM)
REM 4. Set action to Start a Program
REM 5. Program/script: C:\Users\OPTIMUS\financial_data_modelling\schedule_pipeline.bat
REM 6. Finish the wizard