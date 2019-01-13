# ECS Digital USE CASE

Required:

1. Docker
2. python 2.7.10
3. pip packages (mysql-connector-python-rf)

Instructions:

1. `docker-compose up -d`
2. `python ecs_challenge.py db_scripts ecs_user localhost ecs_database ecs_user`

Notes/ observations:

1. Need to handle absolute/ relative paths
2. Would typically use flyway/ liquibase for tasks like this
3. Task took the complete 4 hours, been learning python over Christmas so thought it would be more interesting to use that instead of bash!
4. Haven't handled scripts containing no number, just ignored
5. Example logs provided: `first_run.log` & `second_run.log`
