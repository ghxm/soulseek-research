FROM python:3.11-slim

# Install uv
RUN pip install uv

# Copy project files
COPY . /app
WORKDIR /app

# Install dependencies
RUN uv sync

# Create entrypoint script
RUN echo '#!/bin/bash\n\
uv run soulseek-research --log-level "${LOG_LEVEL:-INFO}" start \\\n\
  --username "$SOULSEEK_USERNAME" \\\n\
  --password "$SOULSEEK_PASSWORD" \\\n\
  --database-url "$DATABASE_URL" \\\n\
  --client-id "${CLIENT_ID:-client}" \\\n\
  --batch-size "${BATCH_SIZE:-10000}" \\\n\
  --max-queue-size "${MAX_QUEUE_SIZE:-100000}" \\\n\
  --encryption-key "$ENCRYPTION_KEY"' > /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# Run the client
CMD ["/app/entrypoint.sh"]