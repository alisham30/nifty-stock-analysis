FROM jupyter/pyspark-notebook:latest

# Set working directory
WORKDIR /home/jovyan/work

# Copy project files
COPY . /home/jovyan/work

# Install Python dependencies
RUN pip install --no-cache-dir streamlit plotly pandas

# Expose both ports: Jupyter (8888) and Streamlit (8501)
EXPOSE 8888
EXPOSE 8501

# Default entrypoint: start Jupyter (so you get the token URL)
CMD ["start-notebook.sh"]
