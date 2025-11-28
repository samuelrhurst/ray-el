FROM mcr.microsoft.com/devcontainers/python:3.13-trixie

# Install UV
# ==========
# If you don't set the install dir to /usr/local/bin, it will install to /root/.local/bin
# and therefore won't work for non-root users in the container.
# /usr/local/bin is already on the path
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates
ADD https://astral.sh/uv/0.9.10/install.sh /uv-installer.sh
RUN UV_INSTALL_DIR=/usr/local/bin sh /uv-installer.sh && rm /uv-installer.sh

# Note: This docker file runs as root, so to make sure any tools installed with uv are
# available to non-root users, we install them to /usr/local/share/uv/tools and
# set the UV_TOOL_BIN_DIR to /usr/local/bin.

# Install MkDocs
# ==============
RUN UV_TOOL_DIR=/usr/local/share/uv/tools UV_TOOL_BIN_DIR=/usr/local/bin uv tool install mkdocs==1.6.1 --with mkdocs-material==9.7.0 --with mkdocstrings[python]==0.30.1 --with mkdocs-autoapi==0.4.1

# Install pytest
# ==============
# Install not required, it's already in the base image.
# RUN UV_TOOL_DIR=/usr/local/share/uv/tools UV_TOOL_BIN_DIR=/usr/local/bin uv tool install pytest==9.0.1