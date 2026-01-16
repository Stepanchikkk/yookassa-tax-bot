FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY bot/ ./bot/

# Create volume for database
VOLUME ["/app/data"]

# Run bot
CMD ["python", "-m", "bot.main"]