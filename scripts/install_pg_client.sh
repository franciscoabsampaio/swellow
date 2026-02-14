#!/bin/bash
sudo apt-get update
sudo apt-get install -y wget gnupg lsb-release

# Add PostgreSQL official repo key
wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql.gpg

# Add PostgreSQL repo
echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/postgresql.list

# Update and install client
sudo apt-get update
sudo apt-get install -y postgresql-client-$(cat PG_VERSION)