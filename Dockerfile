FROM quay.io/astronomer/astro-runtime:10.0.0

# Install dbt and Cosmos
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
