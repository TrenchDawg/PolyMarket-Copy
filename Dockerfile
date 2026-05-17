FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Live trading mode (change to "main.py" without --live for dry run)
CMD ["python", "main.py", "--live"]
