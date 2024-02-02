# Use an official Python runtime as a parent image
FROM python:3.8

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install SQLite3 if not included in your Python image
RUN apt-get update \
    && apt-get install -y libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install any needed packages specified in requirements.txt

RUN pip install -r requirements.txt


# Run your orchestrator script to execute other scripts in order
CMD ["python", "run_scripts.py"]
