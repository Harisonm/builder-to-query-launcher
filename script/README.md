# Information

Using the scripts in this document for crontab

# Crontab Part
```bash
crontab -e
```

Show log of crontab
```
tail -f cron.log
```

## Using date in crontab
début du mois : date +%Y-%m-%d -d "-0 month -$(($(date +%d)-1)) days"
fin du mois : date +%Y-%m-%d -d "-$(date +%d) days +1 month"
date d'hier : date +"%Y-%m-%d" -d "yesterday"