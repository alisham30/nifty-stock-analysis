# Use official Spark + Python base
FROM jupyter/pyspark-notebook:latest

# Set working directory inside container
WORKDIR /home/jovyan/work

# Install system dependencies
USER root
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Switch back to jovyan (non-root user in Jupyter images)
USER jovyan

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code into container
COPY . .

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
