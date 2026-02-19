FROM mcr.microsoft.com/devcontainers/base:debian-12

# dummy data directories
WORKDIR /tmp/projects
WORKDIR /beamlinedata/projects

# set some user and workdir settings to work nicely with vscode
# args may be overridden in devcontainer.json
ARG USER_UID=1000
ARG USER_GID=1000
RUN usermod -u ${USER_UID} vscode
RUN groupmod -g ${USER_GID} vscode

USER vscode
WORKDIR /home/vscode
RUN curl -fsSL https://astral.sh/uv/install.sh | sh
RUN echo 'eval "$(uv generate-shell-completion bash)"' >> /home/vscode/.bashrc

# bash aliases
RUN echo 'alias ll="ls -alF"' >> /home/vscode/.bashrc
