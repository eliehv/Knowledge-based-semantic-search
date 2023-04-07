FROM python:3.8.15-buster

COPY requirements.txt .
#COPY frontend.py .
RUN pip install -r requirements.txt

#RUN mkdir -p /workspace

WORKDIR /workspace
COPY . /workspace

EXPOSE 8600

ENTRYPOINT [ "streamlit", "run" ]
CMD [ "frontend.py"]
