# Deployment Guide - Bus Waypoint Data Platform

This guide provides instructions for deploying the data processing system on Google Cloud Platform (GCP).

## 1. Infrastructure Provisioning

We use GCP **Spot VMs** to balance performance and cost-efficiency. Spot VMs offer significant discounts (60-91%) compared to standard instances.

### GCP Instance Configuration
- **Machine Type**: `e2-highmem-8` (8 vCPUs, 64 GB RAM)
- **OS**: Ubuntu 22.04 LTS
- **Disk**: 100GB SSD (Balanced)
- **Provisioning Model**: SPOT

### Provisioning Script
Run the following command to create the instance:

```bash
gcloud compute instances create instance-docker-platform \
    --project=mp252-492213 \
    --zone=asia-southeast1-b \
    --machine-type=e2-highmem-8 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=TERMINATE \
    --provisioning-model=SPOT \
    --instance-termination-action=STOP \
    --create-disk=auto-delete=yes,boot=yes,device-name=instance-1,image=projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts,mode=rw,size=100,type=pd-balanced \
    --tags=http-server,https-server,docker-node
```

### Network Configuration (Firewall)
To access the administration UIs, open the required ports:

```bash
gcloud compute firewall-rules create allow-dataplatform-ui \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:3000,tcp:8080,tcp:8088,tcp:8089,tcp:8090,tcp:9000,tcp:9001,tcp:9002,tcp:9090 \
    --target-tags=docker-node \
    --source-ranges=[YOUR_IP_ADDRESS]/32
```

## 2. Identity and Access Management (IAM)

Ensure the Service Account attached to the VM has the following permissions if you plan to integrate with other GCP services (like GCS or BigQuery):

- **Storage Admin**: If using Google Cloud Storage as an alternative to MinIO.
- **Compute Viewer**: For instance metadata access.
- **Artifact Registry Reader**: If using private Docker images.

*Recommended: Use a dedicated Service Account with the Principle of Least Privilege.*

## 3. Software Setup

### Install Docker & Docker Compose
Connect to your instance via SSH and run:

```bash
# Update packages
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg

# Add Docker GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Set up repository
echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$ID")" "$(lsb_release -cs)" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

## 4. Deployment

### Clone and Launch
```bash
git clone <your-repo-url>
cd mp252
docker compose up -d
```

### Services Overview:
- **Kafka (KRaft Mode)**: Streaming message broker.
- **Spark (Master & Worker)**: Distributed data processing.
- **MinIO**: S3-compatible storage for Iceberg tables.
- **Gravitino**: Metadata management and Iceberg REST Catalog.

## 5. Administration UIs

Access the following dashboards via `http://<external-ip>:<port>`:

- **Spark Master**: `8080`
- **MinIO Console**: `9001`
- **Gravitino UI**: `8090`
- **Kafka UI**: `8088` (if configured)
- **Prometheus/Grafana**: `9090` / `3000`

## 6. Spot VM Resilience
Since Spot VMs can be reclaimed, ensure your pipeline uses **checkpoints**. You can automate the restart on boot:
```bash
# Add to crontab
@reboot cd /home/admin-user/project/mp252 && /usr/bin/docker compose up -d
```