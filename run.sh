#!/bin/bash
cd /mnt/ssd/apps/anti-fragile-valuation
source venv/bin/activate
python -m afv20.afv_processor >> /mnt/ssd/apps/anti-fragile-valuation/logs/app.log 2>&1
