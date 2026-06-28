import argparse
import math
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CostInputs:
    monthly_hours: float
    node_hourly_price: float
    eks_control_plane_hourly: float
    baseline_worker_replicas: int
    peak_worker_replicas: int
    active_hours_per_day: float
    worker_cpu_request: float
    worker_memory_request_gib: float
    node_allocatable_cpu: float
    node_allocatable_memory_gib: float


def money(value: float) -> str:
    return f"${value:,.2f}"


def calculate(inputs: CostInputs) -> dict[str, float]:
    pods_by_cpu = math.floor(inputs.node_allocatable_cpu / inputs.worker_cpu_request)
    pods_by_memory = math.floor(
        inputs.node_allocatable_memory_gib / inputs.worker_memory_request_gib
    )
    pods_per_node = max(1, min(pods_by_cpu, pods_by_memory))
    baseline_nodes = math.ceil(inputs.baseline_worker_replicas / pods_per_node)
    active_nodes = math.ceil(inputs.peak_worker_replicas / pods_per_node)
    active_fraction = inputs.active_hours_per_day / 24

    baseline_worker_compute = (
        baseline_nodes * inputs.node_hourly_price * inputs.monthly_hours
    )
    keda_worker_compute = (
        active_nodes * inputs.node_hourly_price * inputs.monthly_hours * active_fraction
    )
    control_plane = inputs.eks_control_plane_hourly * inputs.monthly_hours
    savings = baseline_worker_compute - keda_worker_compute
    savings_percent = savings / baseline_worker_compute * 100

    return {
        "pods_per_node": pods_per_node,
        "baseline_nodes": baseline_nodes,
        "active_nodes": active_nodes,
        "active_fraction": active_fraction,
        "baseline_worker_compute": baseline_worker_compute,
        "keda_worker_compute": keda_worker_compute,
        "control_plane": control_plane,
        "baseline_total": baseline_worker_compute + control_plane,
        "keda_total": keda_worker_compute + control_plane,
        "savings": savings,
        "savings_percent": savings_percent,
    }


def render_markdown(inputs: CostInputs, result: dict[str, float]) -> str:
    idle_hours_per_day = 24 - inputs.active_hours_per_day
    return f"""# Cost Comparison: Always-On Workers vs KEDA Scale-to-Zero

This report estimates the monthly worker-node compute impact of scaling event workers to zero when the RabbitMQ queue is idle. The EKS control plane, NAT gateways, storage, RabbitMQ, monitoring, and data transfer are intentionally shown as baseline platform costs that do not disappear when the worker deployment reaches zero replicas.

## Assumptions

| Input | Value |
| --- | ---: |
| Monthly hours | {inputs.monthly_hours:.0f} |
| EKS control plane | {money(inputs.eks_control_plane_hourly)}/hour |
| Worker node price | {money(inputs.node_hourly_price)}/hour |
| Worker CPU request | {inputs.worker_cpu_request:g} vCPU |
| Worker memory request | {inputs.worker_memory_request_gib:g} GiB |
| Node allocatable CPU | {inputs.node_allocatable_cpu:g} vCPU |
| Node allocatable memory | {inputs.node_allocatable_memory_gib:g} GiB |
| Estimated workers per node | {int(result["pods_per_node"])} |
| Always-on workers | {inputs.baseline_worker_replicas} replicas |
| KEDA peak workers | {inputs.peak_worker_replicas} replicas |
| Active processing window | {inputs.active_hours_per_day:g} hours/day |
| Idle scale-to-zero window | {idle_hours_per_day:g} hours/day |

## Monthly Estimate

| Scenario | Worker nodes billed | Worker compute | EKS control plane | Total shown |
| --- | ---: | ---: | ---: | ---: |
| Always-on workers | {int(result["baseline_nodes"])} nodes x 24x7 | {money(result["baseline_worker_compute"])} | {money(result["control_plane"])} | {money(result["baseline_total"])} |
| KEDA scale-to-zero | {int(result["active_nodes"])} nodes during active window | {money(result["keda_worker_compute"])} | {money(result["control_plane"])} | {money(result["keda_total"])} |

## Savings

KEDA scale-to-zero saves **{money(result["savings"])} per month** on the worker-node portion in this model, a **{result["savings_percent"]:.1f}%** reduction for the event-worker compute slice.

The savings become real EC2 bill savings when the cluster has a node provisioner that can remove empty capacity, such as EKS Auto Mode, Karpenter, or Cluster Autoscaler. Without node scale-down, KEDA still protects application capacity and queue latency, but idle node spend may remain.

## Pricing References

- EKS control plane pricing: https://aws.amazon.com/eks/pricing/
- EC2 On-Demand pricing source: https://aws.amazon.com/ec2/pricing/on-demand/

Regenerate with current pricing:

```bash
python scripts/cost_compare.py --node-hourly-price <price> --output reports/cost-comparison.md
```
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate scale-to-zero cost report.")
    parser.add_argument("--monthly-hours", type=float, default=730)
    parser.add_argument("--node-hourly-price", type=float, default=0.0816)
    parser.add_argument("--eks-control-plane-hourly", type=float, default=0.10)
    parser.add_argument("--baseline-worker-replicas", type=int, default=6)
    parser.add_argument("--peak-worker-replicas", type=int, default=6)
    parser.add_argument("--active-hours-per-day", type=float, default=4)
    parser.add_argument("--worker-cpu-request", type=float, default=0.5)
    parser.add_argument("--worker-memory-request-gib", type=float, default=0.5)
    parser.add_argument("--node-allocatable-cpu", type=float, default=1.8)
    parser.add_argument("--node-allocatable-memory-gib", type=float, default=6.8)
    parser.add_argument(
        "--output", type=Path, default=Path("reports/cost-comparison.md")
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    inputs = CostInputs(
        monthly_hours=args.monthly_hours,
        node_hourly_price=args.node_hourly_price,
        eks_control_plane_hourly=args.eks_control_plane_hourly,
        baseline_worker_replicas=args.baseline_worker_replicas,
        peak_worker_replicas=args.peak_worker_replicas,
        active_hours_per_day=args.active_hours_per_day,
        worker_cpu_request=args.worker_cpu_request,
        worker_memory_request_gib=args.worker_memory_request_gib,
        node_allocatable_cpu=args.node_allocatable_cpu,
        node_allocatable_memory_gib=args.node_allocatable_memory_gib,
    )
    result = calculate(inputs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_markdown(inputs, result), encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
