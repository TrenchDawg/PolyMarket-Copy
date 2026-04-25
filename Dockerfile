FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Live trading mode (change to "main.py" without --live for dry run)
# TEMPORARY: --cleanup-preview is on so the first stale-order cleanup cycle
# only logs proposed transitions for the 34 accumulated PENDINGs. Revert this
# CMD back to ["python", "main.py", "--live"] after reviewing preview logs.
CMD ["python", "main.py", "--live", "--cleanup-preview"]
