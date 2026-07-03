# EC2 environment notes

Start / mount / stop commands, credentials, and canonical paths live in **`CLAUDE.md` → "EC2 for heavy lifting"** — that section is authoritative. This file only covers the interactive-session extras.

- **Instance:** `i-0f5b31a268af53938` (`us-east-1`), r6i.8xlarge (256 GB RAM, 32 vCPU), Ubuntu 24.04. Root disk + a separate EBS data volume mounted at `/mnt/data`.
- **Lifecycle:** keep the instance **stopped** between sessions (~$2/hr while running). Do **not** terminate it and do **not** touch the EBS data volume — it holds the canonical datasets.

## Jupyter session (optional, for notebook work)

```bash
# SSH in with a tunnel (get $DNS as in CLAUDE.md)
ssh -i ~/Downloads/polymarket-key.pem -o StrictHostKeyChecking=no -L 8888:localhost:8888 ubuntu@$DNS

tmux new -s work            # tmux survives SSH drops; reattach with: tmux attach -t work
source ~/venv/bin/activate
cd ~/prediction_markets/analysis
jupyter notebook --no-browser --port=8888 --ip=0.0.0.0
# open http://localhost:8888 locally, paste the token
```

DuckDB settings for a big session:

```python
con = get_connection(memory_limit='200GB', threads=16, force_new=True)
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET max_temp_directory_size='400GB'")
```

## Monitoring

```bash
free -h                      # memory
df -h /mnt/data              # data-volume space
du -sh /mnt/data/tmp/        # DuckDB spill size
ps aux | grep jupyter | grep -v grep
```

## Moving files

- Small files: `scp -i ~/Downloads/polymarket-key.pem <src> ubuntu@$DNS:<dst>` (either direction).
- \>100 MB: use the rclone `dropbox:` remote from the instance (see CLAUDE.md).
