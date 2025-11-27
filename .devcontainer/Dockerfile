FROM mcr.microsoft.com/devcontainers/python:3.13-trixie

# Install UV
# ==========
# If you don't set the install dir to /usr/local/bin, it will install to /root/.local/bin
# and therefore won't work for non-root users in the container.
# /usr/local/bin is already on the path
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
ADD https://astral.sh/uv/0.9.10/install.sh /uv-installer.sh
RUN UV_INSTALL_DIR=/usr/local/bin sh /uv-installer.sh && rm /uv-installer.sh

# Installing Zensical
# ===================
RUN uv tool install mkdocs==1.6.1 --with mkdocs-material==9.7.0 --with mkdocstrings[python]==0.30.1 --with mkdocs-autoapi==0.4.1

# Install Quarto
# ==============
# ARG QUARTO_VERSION=1.9.10
# ADD https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-linux-amd64.deb /quarto-linux-amd64.deb
# RUN apt-get install -y --no-install-recommends /quarto-linux-amd64.deb && rm /quarto-linux-amd64.deb

# RUN pip install jupyter ipykernel