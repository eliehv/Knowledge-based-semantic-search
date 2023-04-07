FROM python:3.8.15-buster

COPY requirements.txt .
#COPY app.py .
RUN pip install -r requirements.txt

#RUN mkdir -p /workspace

WORKDIR /workspace
COPY . /workspace

EXPOSE 23457

ENTRYPOINT [ "python", "app.py" ]
CMD [ "-t", "search"]
