FROM python:3.8-slim

# Install used Debian packages, with Ant and Git
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libpq-dev && \
    rm -rf /var/lib/lists/*

# Add the exploit user
RUN useradd -ms /bin/bash exploit

USER exploit
WORKDIR /home/exploit
ENV PATH="/usr/lib/postgresql/9.6/bin:/home/exploit/.local/bin:${PATH}"
COPY ./syspad_monitor/dist/*.whl /home/exploit/.
RUN pip3 install --user /home/exploit/*.whl

CMD ["syspad_monitor", "--version"]