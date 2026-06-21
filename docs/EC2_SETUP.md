# EC2 Setup Guide — Polymarket Analysis

## Instance Details
- **Instance type**: r6i.8xlarge (256GB RAM, 32 vCPU)
- **AMI**: Ubuntu 24.04 LTS
- **Key**: `~/Downloads/polymarket-key.pem`
- **Storage**: 100GB root + 500GB EBS data volume (separate disk)
- **Cost**: ~$2/hr — terminate when done

## After Every Start / Restart

### 1. SSH in with Jupyter tunnel
```bash
ssh -i ~/Downloads/polymarket-key.pem -L 8888:localhost:8888 ubuntu@<PUBLIC-DNS>
```

### 2. Mount the 500GB data volume
```bash
# Find the 500GB disk
lsblk

# Mount it (device name changes between restarts — look for the 500G disk)
sudo mkdir -p /mnt/data
sudo mount /dev/<DEVICE> /mnt/data    # e.g., /dev/nvme1n1 or /dev/nvme0n1

# Verify
ls /mnt/data/pipeline_data/
```

### 3. Start Jupyter in tmux
```bash
tmux new -s pipeline
source ~/venv/bin/activate
cd ~/pipeline/analysis
jupyter notebook --no-browser --port=8888 --ip=0.0.0.0
```
Detach tmux: `Ctrl+B` then `D`

### 4. Open in browser
Go to `http://localhost:8888` and paste the token from Jupyter output.

### 5. Notebook first cell (after imports/connection)
```python
con = get_connection(memory_limit='200GB', threads=16, force_new=True)
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET max_temp_directory_size='400GB'")
```

## Reconnecting

### If SSH disconnects
```bash
# Just reconnect — tmux keeps Jupyter alive
ssh -i ~/Downloads/polymarket-key.pem -L 8888:localhost:8888 ubuntu@<PUBLIC-DNS>
tmux attach -t pipeline
```

### If Jupyter crashes (OOM)
```bash
tmux attach -t pipeline
source ~/venv/bin/activate
cd ~/pipeline/analysis
jupyter notebook --no-browser --port=8888 --ip=0.0.0.0
```

### If instance was stopped/restarted
- Public DNS/IP changes — check EC2 console for new address
- 500GB volume needs remounting (step 2)
- tmux sessions are lost — recreate (step 3)
- Jupyter needs restarting (step 3)

## Key Paths

| What | Path |
|------|------|
| Trades (partitioned parquet) | `/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet` |
| Market resolutions | `/home/ubuntu/pipeline/output/market_resolutions.parquet` |
| Analysis code | `/home/ubuntu/pipeline/analysis/` |
| Notebook | `/home/ubuntu/pipeline/analysis/exploration.ipynb` |
| Pipeline data (raw/deduped/resolved) | `/mnt/data/pipeline_data/` |
| DuckDB temp spill directory | `/mnt/data/tmp/` |
| Python venv | `~/venv/` |

## Config (analysis/config.py on EC2)

```python
TRADES_PARQUET_GLOB = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
DUCKDB_MEMORY_LIMIT = "200GB"
DUCKDB_THREADS = 16
```

## Monitoring

```bash
# Memory usage
free -h

# Disk spill size
du -sh /mnt/data/tmp/

# Is Jupyter alive?
ps aux | grep jupyter | grep -v grep

# Disk space
df -h /mnt/data
```

## Uploading files from laptop
```bash
scp -i ~/Downloads/polymarket-key.pem <LOCAL_FILE> ubuntu@<PUBLIC-DNS>:~/pipeline/analysis/
```

## Downloading results to laptop
```bash
scp -i ~/Downloads/polymarket-key.pem ubuntu@<PUBLIC-DNS>:~/pipeline/analysis/output/<FILE> ~/prediction_markets/analysis/output/
```

## When Done
1. Download any outputs you need
2. **Stop** the instance (EC2 console) if you plan to resume later
3. **Terminate** the instance if fully done — also delete the 500GB EBS volume
