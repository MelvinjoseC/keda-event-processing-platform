# Cost Comparison: Always-On Workers vs KEDA Scale-to-Zero

This report estimates the monthly worker-node compute impact of scaling event workers to zero when the RabbitMQ queue is idle. The EKS control plane, NAT gateways, storage, RabbitMQ, monitoring, and data transfer are intentionally shown as baseline platform costs that do not disappear when the worker deployment reaches zero replicas.

## Assumptions

| Input | Value |
| --- | ---: |
| Monthly hours | 730 |
| EKS control plane | $0.10/hour |
| Worker node price | $0.08/hour |
| Worker CPU request | 0.5 vCPU |
| Worker memory request | 0.5 GiB |
| Node allocatable CPU | 1.8 vCPU |
| Node allocatable memory | 6.8 GiB |
| Estimated workers per node | 3 |
| Always-on workers | 6 replicas |
| KEDA peak workers | 6 replicas |
| Active processing window | 4 hours/day |
| Idle scale-to-zero window | 20 hours/day |

## Monthly Estimate

| Scenario | Worker nodes billed | Worker compute | EKS control plane | Total shown |
| --- | ---: | ---: | ---: | ---: |
| Always-on workers | 2 nodes x 24x7 | $119.14 | $73.00 | $192.14 |
| KEDA scale-to-zero | 2 nodes during active window | $19.86 | $73.00 | $92.86 |

## Savings

KEDA scale-to-zero saves **$99.28 per month** on the worker-node portion in this model, a **83.3%** reduction for the event-worker compute slice.

The savings become real EC2 bill savings when the cluster has a node provisioner that can remove empty capacity, such as EKS Auto Mode, Karpenter, or Cluster Autoscaler. Without node scale-down, KEDA still protects application capacity and queue latency, but idle node spend may remain.

## Pricing References

- EKS control plane pricing: https://aws.amazon.com/eks/pricing/
- EC2 On-Demand pricing source: https://aws.amazon.com/ec2/pricing/on-demand/

Regenerate with current pricing:

```bash
python scripts/cost_compare.py --node-hourly-price <price> --output reports/cost-comparison.md
```
