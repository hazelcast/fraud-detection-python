FROM ubuntu:22.04

#runtime variables
RUN mkdir /usr/lib/hazelcast
ARG HZ_HOME="/usr/lib/hazelcast"

#Workdir 
WORKDIR ${HZ_HOME}

USER root
RUN apt-get update -y
RUN apt-get install -y apt-transport-https
RUN apt-get install -y gpg-agent
RUN apt-get install -y wget
RUN apt update -y
RUN apt upgrade -y
RUN apt install -y bash-completion

#Install hazelcast 5.3.1
RUN wget -qO - https://repository.hazelcast.com/api/gpg/key/public | gpg --dearmor |  tee /usr/share/keyrings/hazelcast-archive-keyring.gpg > /dev/null
RUN echo "deb [signed-by=/usr/share/keyrings/hazelcast-archive-keyring.gpg] https://repository.hazelcast.com/debian stable main" | tee -a /etc/apt/sources.list
RUN apt update -y && apt install -y hazelcast=5.3.1

#Install Python 3.10
RUN apt-get install -y software-properties-common
RUN apt update -y 
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt install -y python3.10
#Install pip for Python 3
RUN apt update -y
RUN apt install -y python3-pip
RUN pip install --upgrade pip
RUN pip install lightgbm

#Install python3 virtualEnv
RUN apt install -y python3.10-venv

# Avoid "Couldn't 'chmod -w' these files: [jet_to_python_env/bin/python3.10, jet_to_python_env/bin/python, jet_to_python_env/bin/python3]"
RUN chmod -R 777 /usr/lib/hazelcast

# Avoid "lock: cannot open lock file /nonexistent/.jet-pip.lock: No such file or directory"
RUN mkdir /nonexistent
RUN chmod -R 777 /nonexistent

# Default Runtime variables
ENV HZ_HOME="${HZ_HOME}" \
    CLASSPATH_DEFAULT="${HZ_HOME}/*" \
    JAVA_OPTS_DEFAULT="-Djava.net.preferIPv4Stack=true -XX:MaxRAMPercentage=80.0 -XX:MaxGCPauseMillis=5" \
    PROMETHEUS_PORT="" \
    PROMETHEUS_CONFIG="${HZ_HOME}/config/jmx_agent_config.yaml" \
    CLASSPATH="" \
    JAVA_OPTS="" \
    HAZELCAST_CONFIG="" \
    LANG=C.UTF-8 \
    PATH=${HZ_HOME}/bin:$PATH

# Expose port
EXPOSE 5701

# User hazelcast
USER hazelcast

CMD ["hz", "start"]