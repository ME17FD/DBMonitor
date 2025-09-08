FROM postgres:15

ENV DEBIAN_FRONTEND=noninteractive

# Install SSH server
RUN apt-get update && \
    apt-get install -y openssh-server && \
    echo 'root:rootpass' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    mkdir -p /var/run/sshd && \
    rm -rf /var/lib/apt/lists/*

EXPOSE 22 5432

# Start SSH in foreground + Postgres
CMD ["/bin/bash", "-c", "/usr/sbin/sshd && docker-entrypoint.sh postgres"]
