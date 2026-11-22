# Prometheus and Grafana monitoring

This stack monitors Docker container CPU, memory, and network usage with cAdvisor, plus Linux host CPU, memory, and filesystem usage with node-exporter. Grafana provisions the dashboard and Prometheus data source automatically.

## Run locally

From the repository root, start the services you want to monitor, then run:

```powershell
docker compose -f monitoring\docker-compose.yml up -d
```

Open Grafana at <http://localhost:3000> and sign in with `admin` / `admin` on first run. Change the password in Grafana or set `GRAFANA_ADMIN_PASSWORD` in the shell before starting the stack. The dashboard is in **Infrastructure > Service & EC2 Infrastructure**. Prometheus is available at <http://localhost:9090>.

cAdvisor discovers containers through the Docker socket/host mounts and Prometheus scrapes it every 15 seconds. The root Compose file binds Grafana and Prometheus to loopback only; do not expose either service directly to the internet.

On Docker Desktop for Windows, Linux containers run inside Docker Desktop's Linux VM. cAdvisor can report those containers, but node-exporter reports the Linux VM rather than the Windows host. For Windows host metrics, install Microsoft's `windows_exporter` on the host and add `host.docker.internal:9182` as a Prometheus target using the Windows exporter dashboard/metrics; this Linux host dashboard's node-exporter queries won't apply to Windows exporter metrics.

## Scrape ECS on EC2

Run cAdvisor and node-exporter on every ECS EC2 instance (for example, as ECS daemon services using host networking). cAdvisor needs read access to the Docker socket, `/sys`, and the Docker data directory; node-exporter needs host `/proc`, `/sys`, and root filesystem access. Use least privilege, pin approved image versions, and restrict exporter ingress to the Prometheus security group. Do not expose exporter ports publicly.

Prometheus EC2 service discovery is enabled for the `ap-southeast-2` region and automatically discovers every running EC2 instance. Change the region in both `ec2_sd_configs` entries if this ECS cluster uses another region. Prometheus discovers instances as soon as they enter the running state and drops them after termination.

EC2 discovery requires the Prometheus host/task to have an IAM principal with `ec2:DescribeInstances`. On ECS, attach that permission to the Prometheus task role. For a local Prometheus container using AWS discovery, configure a credential chain through environment variables or an AWS profile; no AWS credentials are required for local Docker targets.

EC2 discovery is preferred for an ASG. It scrapes every running instance in the configured region, so instances do not need to be added manually. To limit discovery to one ASG, add an EC2 tag filter such as:

```yaml
filters:
  - name: instance-state-name
    values: ["running"]
  - name: tag:aws:autoscaling:groupName
    values: ["YOUR-ASG-NAME"]
```

Add `amazon-ecs-managed` or another cluster-specific tag when more than one ASG or cluster shares the region. This prevents Prometheus from polling unrelated EC2 hosts.

Static file discovery remains available as a fallback. Add each instance's **private** address to the corresponding file-discovery file:

* `prometheus\aws-ecs-cadvisor-targets.json`: port `8080`
* `prometheus\aws-ecs-node-targets.json`: port `9100`

Example:

```json
[
  {
    "targets": ["10.0.1.23:8080"],
    "labels": {
      "environment": "production",
      "instance": "ecs-host-1"
    }
  }
]
```

The static files are watched by Prometheus, so target changes take effect without restarting it. They are unnecessary when EC2 service discovery is configured correctly.

cAdvisor exposes ECS container metrics from the host, so the dashboard covers all tasks on each monitored EC2 instance. Its service labels use Docker Compose service names locally and ECS container names when present. Ensure the ECS container runtime retains labels. The included monitoring stack is intended to run on a trusted host; for AWS production use, deploy Prometheus/Grafana with persistent storage, authentication, TLS, restricted security groups, and an appropriate high-availability/retention plan.
