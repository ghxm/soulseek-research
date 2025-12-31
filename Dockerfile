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
uv run soulseek-research start \\\n\
  --username "$SOULSEEK_USERNAME" \\\n\
  --password "$SOULSEEK_PASSWORD" \\\n\
  --database-url "$DATABASE_URL" \\\n\
  --client-id "${CLIENT_ID:-client}"' > /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

# Run the client
CMD ["/app/entrypoint.sh"]