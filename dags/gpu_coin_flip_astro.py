"""
GPU Coin Flip (Astro native GPU support)

Flips 1,000,000 coins on an NVIDIA GPU and files a summary report.

Unlike a self-managed setup, there is no provision/deprovision step here:
Astro manages the GPU node pools in the dedicated cluster. Requesting
`nvidia.com/gpu` in the KubernetesPodOperator container resources is enough
for Astro to schedule the Pod onto a GPU node - no ASG scaling, node
selectors, or tolerations required.

Docs: https://www.astronomer.io/docs/astro/gpu-support-astro

Pipeline:
  run_coin_flip (KPO on GPU) -> file_report
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag, task
from kubernetes.client import models as k8s

# The Deployment's own namespace, resolved at runtime on Astro.
NAMESPACE = conf.get("kubernetes", "NAMESPACE", fallback="default")

N_FLIPS = 1_000_000

COIN_FLIP_SCRIPT = f"""
import json, os, time
import torch

print("=" * 58)
print("  GPU COIN FLIP")
print("=" * 58)
cuda = torch.cuda.is_available()
device = torch.device("cuda" if cuda else "cpu")
print(f"  Device : {{device}}")
if cuda:
    print(f"  GPU    : {{torch.cuda.get_device_name(0)}}")
    print(f"  CUDA   : {{torch.version.cuda}}")
print("=" * 58)

if not cuda:
    raise SystemExit("No CUDA device visible in the Pod - GPU was not allocated.")

N = {N_FLIPS}

torch.cuda.synchronize()
t0 = time.time()
flips = torch.randint(0, 2, (N,), device=device)
heads = int((flips == 1).sum())
tails = N - heads
torch.cuda.synchronize()
elapsed = time.time() - t0

print(f"\\nFlipped {{N:,}} coins in {{elapsed:.4f}}s")
print(f"  Heads : {{heads:,}}")
print(f"  Tails : {{tails:,}}")

os.makedirs("/airflow/xcom", exist_ok=True)
with open("/airflow/xcom/return.json", "w") as f:
    json.dump(
        {{
            "heads": heads,
            "tails": tails,
            "n": N,
            "elapsed_sec": round(elapsed, 4),
            "gpu": torch.cuda.get_device_name(0),
        }},
        f,
    )
print("\\nResults written to XCom.")
"""


@dag(
    dag_id="gpu_coin_flip_astro",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # trigger manually from the Airflow UI
    catchup=False,
    tags=["gpu", "demo", "coin-flip", "astro-gpu-labs"],
)
def gpu_coin_flip_astro():
    run_coin_flip = KubernetesPodOperator(
        task_id="run_coin_flip",
        name="gpu-coin-flip",
        namespace=NAMESPACE,
        image="pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime",
        cmds=["python", "-c"],
        arguments=[COIN_FLIP_SCRIPT],
        # The only GPU-specific setting: request whole GPUs. Astro detects the
        # nvidia.com/gpu request and places the Pod on a GPU node.
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "2", "memory": "8Gi", "nvidia.com/gpu": "1"},
            limits={"cpu": "3", "memory": "12Gi", "nvidia.com/gpu": "1"},
        ),
        in_cluster=True,
        get_logs=True,
        do_xcom_push=True,
        on_finish_action="delete_pod",
        # Allow time for a GPU node to scale up and pull the ~3GB PyTorch image.
        startup_timeout_seconds=900,
        execution_timeout=timedelta(minutes=30),
        retries=0,  # surface GPU scheduling failures directly while testing
    )

    @task
    def file_report(result: dict) -> None:
        n = result["n"]
        heads = result["heads"]
        tails = result["tails"]
        print("Coin flip report")
        print(f"  GPU         : {result['gpu']}")
        print(f"  Total flips : {n:,}")
        print(f"  Heads       : {heads:,} ({heads / n * 100:.2f}%)")
        print(f"  Tails       : {tails:,} ({tails / n * 100:.2f}%)")
        print(f"  GPU time    : {result['elapsed_sec']}s")

    file_report(run_coin_flip.output)


gpu_coin_flip_astro()
