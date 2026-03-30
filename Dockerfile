FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Default: dry run mode
# Override with Railway env var: COMMAND=--live
CMD ["sh", "-c", "python main.py ${COMMAND:-}"]
