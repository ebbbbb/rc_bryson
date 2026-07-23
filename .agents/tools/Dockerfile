FROM python:3.12.10-alpine@sha256:4bbf5ef9ce4b273299d394de268ad6018e10a9375d7efc7c2ce9501a6eb6b86c

RUN pip install --no-cache-dir PyYAML==6.0.2
COPY .agents/tools/quick_validate.py /opt/skill-creator/quick_validate.py
ENTRYPOINT ["python3", "/opt/skill-creator/quick_validate.py"]
