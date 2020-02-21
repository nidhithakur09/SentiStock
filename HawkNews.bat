@echo off
call C:\ProgramData\Anaconda3\Scripts\activate.bat
:START
cls
echo "Starting Python Main Script"
python "RSS_News.py"
cls
echo "Starting Attention Script"
python "Attention.py"
timeout 1800
GOTO START