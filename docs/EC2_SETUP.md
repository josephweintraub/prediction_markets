# EC2 environment and lifecycle

Operational reference for the shared production instance. `AGENTS.md` governs ownership: subagents never manage instance or storage lifecycle; the root agent does so only after confirming no active agent or process still needs the instance.

## Environment

- Instance: `i-0f5b31a268af53938`, `us-east-1`, Ubuntu 24.04. AWS reported
  `r5.8xlarge` on 2026-10-24; query the actual type rather than assuming it is fixed.
- AWS CLI profile: `claude-ec2` (the name is retained configuration, not an agent assignment).
- Local SSH key: `~/Downloads/polymarket-key.pem` (mode 600; never replace or commit it).
- Canonical repository: `/home/ubuntu/prediction_markets`.
- Python: `/home/ubuntu/venv/bin/python`.
- Canonical data: `/mnt/data`; never terminate the instance or alter/detach the EBS volume.
- The public DNS changes after a restart. Use `StrictHostKeyChecking=no` and `LogLevel=ERROR` for this ephemeral research host.

## Start and connect

```bash
aws --profile claude-ec2 ec2 start-instances --instance-ids i-0f5b31a268af53938
aws --profile claude-ec2 ec2 wait instance-status-ok --instance-ids i-0f5b31a268af53938
INSTANCE_DNS=$(aws --profile claude-ec2 ec2 describe-instances \
  --instance-ids i-0f5b31a268af53938 \
  --query 'Reservations[0].Instances[0].PublicDnsName' --output text)
ssh -i ~/Downloads/polymarket-key.pem \
  -o StrictHostKeyChecking=no -o LogLevel=ERROR ubuntu@$INSTANCE_DNS
```

After a restart, identify the data volume with `lsblk -f` and `findmnt /mnt/data`.
The verified data-volume UUID is `d0cd087b-94c4-428c-bae9-ae28929059f6` (2026-10-24).
Device names can alternate. Never try successive guessed device names.

```bash
ssh -i ~/Downloads/polymarket-key.pem \
  -o StrictHostKeyChecking=no -o LogLevel=ERROR ubuntu@$INSTANCE_DNS \
  'mountpoint -q /mnt/data || sudo mount -U d0cd087b-94c4-428c-bae9-ae28929059f6 /mnt/data'
```

Confirm that `findmnt /mnt/data` reports the expected volume before reading data.
Stop and ask if the UUID is absent, already mounted elsewhere, or does not match
the recorded volume; do not format, repair or mount a different disk.

Before any production command, verify the canonical checkout, clean state, expected commit, input paths, and that the output run directory does not already exist.

## Stop

The instance incurs compute charges while running. If the root started or used it for a task, shutdown is part of completing that task even after failure or interruption. The root must safely finish any required data handling, confirm that no agent, production process, notebook, or transfer still needs the instance, stop it, and verify the stopped state:

```bash
aws --profile claude-ec2 ec2 stop-instances --instance-ids i-0f5b31a268af53938
aws --profile claude-ec2 ec2 wait instance-stopped --instance-ids i-0f5b31a268af53938
```

Routine stopping does not require unmounting or detaching the EBS volume.

## Jupyter session

```bash
ssh -i ~/Downloads/polymarket-key.pem \
  -o StrictHostKeyChecking=no -L 8888:localhost:8888 ubuntu@$INSTANCE_DNS

tmux new -s work
source ~/venv/bin/activate
cd ~/prediction_markets/analysis
jupyter notebook --no-browser --port=8888 --ip=0.0.0.0
```

For a large DuckDB session:

```python
con = get_connection(memory_limit="200GB", threads=16, force_new=True)
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET max_temp_directory_size='400GB'")
```

## Monitor and transfer

```bash
free -h
df -h /mnt/data
du -sh /mnt/data/tmp/
ps aux | grep jupyter | grep -v grep
```

- Small files: use `scp` with the SSH options above.
- Files larger than 100 MB: use the instance’s `rclone` remote, for example `rclone copy /mnt/data/some.parquet "dropbox:Polymarket Data and Code/some/"`.
- Dropbox is for finished deliverables only; keep drafts and scratch files in the run or working directory.
- Never echo or copy service credentials from the instance.
