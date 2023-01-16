FROM python:3.8-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /builder-to-bq
WORKDIR $APP_HOME
COPY . ./
# COPY /requirements.txt ./
# COPY /resources ./
# Install production dependencies.
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 80
CMD ["uvicorn", "builder_to_bq:app", "--host", "0.0.0.0", "--port", "80"]
#ENTRYPOINT ["python","-m","src.main"]