# Edit this file to introduce tasks to be run by cron.
#
# Each task to run has to be defined through a single line
# indicating with different fields when the task will be run
# and what command to run for the task
#
# To define the time you can provide concrete values for
# minute (m), hour (h), day of month (dom), month (mon),
# and day of week (dow) or use '*' in these fields (for 'any').#
# Notice that tasks will be started based on the cron's system
# daemon's notion of time and timezones.
#
# Output of the crontab jobs (including errors) is sent through
# email to the user the crontab file belongs to (unless redirected).
#
# For example, you can run a backup of all your user accounts
# at 5 a.m every week with:
# 0 5 * * 1 tar -zcf /var/backups/home.tgz /home/
#
# For more information see the manual pages of crontab(5) and cron(8)

0 7 1 * * /bin/bash /home/publisher/builder-to-bq/current/script/clients.sh 2>&1 | /usr/bin/slacktee -c "fd-retailink" >> /home/publisher/builder-to-bq/log/client.log 2>&1
0 7 * * * /bin/bash /home/publisher/builder-to-bq/current/script/ecommerce.sh 2>&1 | /usr/bin/slacktee -c "fd-retailink" >> /home/publisher/builder-to-bq/log/ecommerce.log 2>&1
0 7 * * * /bin/bash /home/publisher/builder-to-bq/current/script/ecommerce_mot_cle.sh 2>&1 | /usr/bin/slacktee -c "fd-retailink" >> /home/publisher/builder-to-bq/log/ecommerce_mot_cle.log 2>&1
0 7 * * * /bin/bash /home/publisher/builder-to-bq/current/script/ventes.sh 2>&1 | /usr/bin/slacktee -c "fd-retailink" >> /home/publisher/builder-to-bq/log/ventes.log 2>&1
45 6 * * * /bin/bash /home/publisher/builder-to-bq/current/script/referential_vente_product.sh 2>&1 | /usr/bin/slacktee -c "fd-retailink" >> /home/publisher/builder-to-bq/log/referential_vente_product.log 2>&1
0 7 * * * /bin/bash /home/publisher/builder-to-bq/current/script/stocks.sh 2>&1 | /usr/bin/slacktee -c "fd-retailink" >> /home/publisher/builder-to-bq/log/stocks.log 2>&1

45 6 * * * /bin/bash /opt/tableau/tabcmd/bin/tabcmd login -s https://62-4-22-116.rev.poneytelecom.eu -u admint -p bw63PZ7Meb8R 2>&1 | /usr/bin/slacktee -c "fd-retailink"
# 15 7 * * * /opt/tableau/tabcmd/bin/tabcmd refreshextracts --project "fnac-darty-data-sharing" --datasource insights_ventes\ \(datamart\) 2>&1 | /usr/bin/slacktee -c "fd-retailink"
15 7 * * * /opt/tableau/tabcmd/bin/tabcmd refreshextracts --project "fnac-darty-data-sharing" --datasource insights_ecommerce\ \(datamart\) 2>&1 | /usr/bin/slacktee -c "fd-retailink"
15 7 * * * /opt/tableau/tabcmd/bin/tabcmd refreshextracts --project "fnac-darty-data-sharing" --datasource insights_ecommerces_mot_cle\ \(datamart\) 2>&1 | /usr/bin/slacktee -c "fd-retailink"
# 15 7 * * * /opt/tableau/tabcmd/bin/tabcmd refreshextracts --project "fnac-darty-data-sharing" --datasource insights_stock\ \(datamart\) 2>&1 | /usr/bin/slacktee -c "fd-retailink"
15 7 1 * * /opt/tableau/tabcmd/bin/tabcmd refreshextracts --project "fnac-darty-data-sharing" --datasource insights_profiling_clients\ \(datamart\) 2>&1 | /usr/bin/slacktee -c "fd-retailink"
15 7 * * * /opt/tableau/tabcmd/bin/tabcmd refreshextracts --project "fnac-darty-data-sharing" --datasource referential_stock_user_fournisseur 2>&1 | /usr/bin/slacktee -c "fd-retailink"
15 7 * * * /opt/tableau/tabcmd/bin/tabcmd refreshextracts --project "fnac-darty-data-sharing" --datasource referential_vente_user_fournisseur\ \  2>&1 | /usr/bin/slacktee -c "fd-retailink"
