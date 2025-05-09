# Use the Jupyter Data Science Notebook as the base image
FROM jupyter/datascience-notebook

# Set the working directory to /home/jovyan - the default for Jupyter images
WORKDIR /home/jovyan

EXPOSE 8888

# Avoid prompts from apt during the build process
ENV DEBIAN_FRONTEND=noninteractive

# install additional Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy CSV files
COPY Data/*.csv /home/jovyan/data/

# Copy notebook in
# COPY Homework_02.ipynb /home/jovyan/

# Change ownership
USER root
RUN chown -R jovyan:users /home/jovyan/
USER jovyan

# Start the Jupyter Notebook server when the container launches
# The base image already configures Jupyter to run on start, but you can customize
# startup options here if needed.
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''", "--allow-root"]

